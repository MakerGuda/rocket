package org.apache.rocketmq.broker.controller;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.autoswitch.BrokerMetadata;
import org.apache.rocketmq.store.ha.autoswitch.TempBrokerMetadata;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.rocketmq.remoting.protocol.ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST;

@Getter
@Setter
public class ReplicasManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final int RETRY_INTERVAL_SECOND = 5;

    private final ScheduledExecutorService scheduledService;

    private final ExecutorService executorService;

    private final ExecutorService scanExecutor;

    private final BrokerController brokerController;

    private final AutoSwitchHAService haService;

    private final BrokerConfig brokerConfig;

    private final String brokerAddress;

    private final BrokerOuterAPI brokerOuterAPI;

    private final ConcurrentMap<String, Boolean> availableControllerAddresses;

    private List<String> controllerAddresses;

    private volatile String controllerLeaderAddress = "";

    private volatile State state = State.INITIAL;

    private volatile RegisterState registerState = RegisterState.INITIAL;

    private ScheduledFuture<?> checkSyncStateSetTaskFuture;

    private ScheduledFuture<?> slaveSyncFuture;

    private Long brokerControllerId;

    private Long masterBrokerId;

    private BrokerMetadata brokerMetadata;

    private TempBrokerMetadata tempBrokerMetadata;

    private Set<Long> syncStateSet;

    private int syncStateSetEpoch = 0;

    private String masterAddress = "";

    private int masterEpoch = 0;

    private long lastSyncTimeMs = System.currentTimeMillis();

    private Random random = new Random();

    public ReplicasManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
        this.scheduledService = ThreadUtils.newScheduledThreadPool(3, new ThreadFactoryImpl("ReplicasManager_ScheduledService_", brokerController.getBrokerIdentity()));
        this.executorService = ThreadUtils.newThreadPoolExecutor(3, new ThreadFactoryImpl("ReplicasManager_ExecutorService_", brokerController.getBrokerIdentity()));
        this.scanExecutor = ThreadUtils.newThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("ReplicasManager_scan_thread_", brokerController.getBrokerIdentity()));
        this.haService = (AutoSwitchHAService) brokerController.getMessageStore().getHaService();
        this.brokerConfig = brokerController.getBrokerConfig();
        this.availableControllerAddresses = new ConcurrentHashMap<>();
        this.syncStateSet = new HashSet<>();
        this.brokerAddress = brokerController.getBrokerAddr();
        this.brokerMetadata = new BrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity());
        this.tempBrokerMetadata = new TempBrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity() + "-temp");
    }

    public void start() {
        this.state = State.INITIAL;
        updateControllerAddr();
        scanAvailableControllerAddresses();
        this.scheduledService.scheduleAtFixedRate(this::updateControllerAddr, 2 * 60 * 1000, 2 * 60 * 1000, TimeUnit.MILLISECONDS);
        this.scheduledService.scheduleAtFixedRate(this::scanAvailableControllerAddresses, 3 * 1000, 3 * 1000, TimeUnit.MILLISECONDS);
        if (!startBasicService()) {
            LOGGER.error("Failed to start replicasManager");
            this.executorService.submit(() -> {
                int retryTimes = 0;
                do {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECOND);
                    } catch (InterruptedException ignored) {

                    }
                    retryTimes++;
                    LOGGER.warn("Failed to start replicasManager, retry times:{}, current state:{}, try it again", retryTimes, this.state);
                }
                while (!startBasicService());
                LOGGER.info("Start replicasManager success, retry times:{}", retryTimes);
            });
        }
    }

    public boolean startBasicService() {
        if (this.state == State.SHUTDOWN)
            return false;
        if (this.state == State.INITIAL) {
            if (schedulingSyncControllerMetadata()) {
                this.state = State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE;
                LOGGER.info("First time sync controller metadata success, change state to: {}", this.state);
            } else {
                return false;
            }
        }
        if (this.state == State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE) {
            for (int retryTimes = 0; retryTimes < 5; retryTimes++) {
                if (register()) {
                    this.state = State.REGISTER_TO_CONTROLLER_DONE;
                    LOGGER.info("First time register broker success, change state to: {}", this.state);
                    break;
                }
                try {
                    Thread.sleep(random.nextInt(1000));
                } catch (Exception ignore) {

                }
            }
            if (this.state != State.REGISTER_TO_CONTROLLER_DONE) {
                LOGGER.error("Register to broker failed 5 times");
                return false;
            }
        }
        if (this.state == State.REGISTER_TO_CONTROLLER_DONE) {
            this.sendHeartbeatToController();
            if (this.masterBrokerId != null || brokerElect()) {
                LOGGER.info("Master in this broker set is elected, masterBrokerId: {}, masterBrokerAddr: {}", this.masterBrokerId, this.masterAddress);
                this.state = State.RUNNING;
                setFenced(false);
                LOGGER.info("All register process has been done, change state to: {}", this.state);
            } else {
                return false;
            }
        }
        schedulingSyncBrokerMetadata();
        this.haService.registerSyncStateSetChangedListener(this::doReportSyncStateSetChanged);
        return true;
    }

    public void shutdown() {
        this.state = State.SHUTDOWN;
        this.registerState = RegisterState.INITIAL;
        this.executorService.shutdownNow();
        this.scheduledService.shutdownNow();
        this.scanExecutor.shutdownNow();
    }

    public synchronized void changeBrokerRole(final Long newMasterBrokerId, final String newMasterAddress, final Integer newMasterEpoch, final Integer syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        if (newMasterBrokerId != null && newMasterEpoch > this.masterEpoch) {
            if (newMasterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(newMasterEpoch, syncStateSetEpoch, syncStateSet);
            } else {
                changeToSlave(newMasterAddress, newMasterEpoch, newMasterBrokerId);
            }
        }
    }

    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to master, brokerName:{}, replicas:{}, new Epoch:{}", this.brokerConfig.getBrokerName(), this.brokerAddress, newMasterEpoch);
                this.masterEpoch = newMasterEpoch;
                if (this.masterBrokerId != null && this.masterBrokerId.equals(this.brokerControllerId) && this.brokerController.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
                    final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                    changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);
                    this.haService.changeToMasterWhenLastRoleIsMaster(newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    this.executorService.submit(this::checkSyncStateSetAndDoReport);
                    registerBrokerWhenRoleChange();
                    return;
                }
                final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);
                handleSlaveSynchronize(BrokerRole.SYNC_MASTER);
                this.haService.changeToMaster(newMasterEpoch);
                this.brokerController.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);
                this.brokerController.changeSpecialServiceStatus(true);
                this.masterAddress = this.brokerAddress;
                this.masterBrokerId = this.brokerControllerId;
                schedulingCheckSyncStateSet();
                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                this.executorService.submit(this::checkSyncStateSetAndDoReport);
                registerBrokerWhenRoleChange();
            }
        }
    }

    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, Long newMasterBrokerId) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to slave, brokerName={}, brokerId={}, newMasterBrokerId={}, newMasterAddress={}, newMasterEpoch={}", this.brokerConfig.getBrokerName(), this.brokerControllerId, newMasterBrokerId, newMasterAddress, newMasterEpoch);
                this.masterEpoch = newMasterEpoch;
                if (newMasterBrokerId.equals(this.masterBrokerId)) {
                    this.haService.changeToSlaveWhenMasterNotChange(newMasterAddress, newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    registerBrokerWhenRoleChange();
                    return;
                }
                stopCheckSyncStateSet();
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
                this.brokerController.changeSpecialServiceStatus(false);
                this.brokerConfig.setBrokerId(brokerControllerId);
                this.masterAddress = newMasterAddress;
                this.masterBrokerId = newMasterBrokerId;
                handleSlaveSynchronize(BrokerRole.SLAVE);
                this.haService.changeToSlave(newMasterAddress, newMasterEpoch, brokerControllerId);
                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                registerBrokerWhenRoleChange();
            }
        }
    }

    public void registerBrokerWhenRoleChange() {
        this.executorService.submit(() -> {
            try {
                this.brokerController.registerBrokerAll(true, false, this.brokerController.getBrokerConfig().isForceRegister());
            } catch (final Throwable e) {
                LOGGER.error("Error happen when register broker to name-srv, Failed to change broker to {}", this.brokerController.getMessageStoreConfig().getBrokerRole(), e);
                return;
            }
            LOGGER.info("Change broker [id:{}][address:{}] to {}, newMasterBrokerId:{}, newMasterAddress:{}, newMasterEpoch:{}, syncStateSetEpoch:{}", this.brokerControllerId, this.brokerAddress, this.brokerController.getMessageStoreConfig().getBrokerRole(), this.masterBrokerId, this.masterAddress, this.masterEpoch, this.syncStateSetEpoch);
        });

    }

    private void changeSyncStateSet(final Set<Long> newSyncStateSet, final int newSyncStateSetEpoch) {
        synchronized (this) {
            if (newSyncStateSetEpoch > this.syncStateSetEpoch) {
                LOGGER.info("SyncStateSet changed from {} to {}", this.syncStateSet, newSyncStateSet);
                this.syncStateSetEpoch = newSyncStateSetEpoch;
                this.syncStateSet = new HashSet<>(newSyncStateSet);
                this.haService.setSyncStateSet(newSyncStateSet);
            }
        }
    }

    private void handleSlaveSynchronize(final BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(this.masterAddress);
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                        brokerController.getSlaveSynchronize().syncAll();
                        lastSyncTimeMs = System.currentTimeMillis();
                    }
                    brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                } catch (final Throwable e) {
                    LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);
        } else {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    private boolean brokerElect() {
        try {
            Pair<ElectMasterResponseHeader, Set<Long>> tryElectResponsePair = this.brokerOuterAPI.brokerElect(this.controllerLeaderAddress, this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerControllerId);
            ElectMasterResponseHeader tryElectResponse = tryElectResponsePair.getObject1();
            Set<Long> syncStateSet = tryElectResponsePair.getObject2();
            final String masterAddress = tryElectResponse.getMasterAddress();
            final Long masterBrokerId = tryElectResponse.getMasterBrokerId();
            if (StringUtils.isEmpty(masterAddress) || masterBrokerId == null) {
                LOGGER.warn("Now no master in broker set");
                return false;
            }
            if (masterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(tryElectResponse.getMasterEpoch(), tryElectResponse.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, tryElectResponse.getMasterEpoch(), tryElectResponse.getMasterBrokerId());
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to try elect", e);
            return false;
        }
    }

    public void sendHeartbeatToController() {
        final List<String> controllerAddresses = this.getAvailableControllerAddresses();
        for (String controllerAddress : controllerAddresses) {
            if (StringUtils.isNotEmpty(controllerAddress)) {
                this.brokerOuterAPI.sendHeartbeatToController(controllerAddress, this.brokerConfig.getBrokerClusterName(), this.brokerAddress, this.brokerConfig.getBrokerName(), this.brokerControllerId, this.brokerConfig.getSendHeartbeatTimeoutMillis(), this.brokerConfig.isInBrokerContainer(), this.getLastEpoch(), this.brokerController.getMessageStore().getMaxPhyOffset(), this.brokerController.getMessageStore().getConfirmOffset(), this.brokerConfig.getControllerHeartBeatTimeoutMills(), this.brokerConfig.getBrokerElectionPriority());
            }
        }
    }

    private boolean register() {
        try {
            confirmNowRegisteringState();
            LOGGER.info("Confirm now register state: {}", this.registerState);
            if (!checkMetadataValid()) {
                LOGGER.error("Check and find that metadata/tempMetadata invalid, you can modify the broker config to make them valid");
                return false;
            }
            if (this.registerState == RegisterState.INITIAL) {
                Long nextBrokerId = getNextBrokerId();
                if (nextBrokerId == null || !createTempMetadataFile(nextBrokerId)) {
                    LOGGER.error("Failed to create temp metadata file, nextBrokerId: {}", nextBrokerId);
                    return false;
                }
                this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
                LOGGER.info("Register state change to {}, temp metadata: {}", this.registerState, this.tempBrokerMetadata);
            }
            if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
                if (!applyBrokerId()) {
                    this.tempBrokerMetadata.clear();
                    this.registerState = RegisterState.INITIAL;
                    LOGGER.info("Register state change to: {}", this.registerState);
                    return false;
                }
                if (!createMetadataFileAndDeleteTemp()) {
                    LOGGER.error("Failed to create metadata file and delete temp metadata file, temp metadata: {}", this.tempBrokerMetadata);
                    return false;
                }
                this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
                LOGGER.info("Register state change to: {}, metadata: {}", this.registerState, this.brokerMetadata);
            }
            if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
                if (!registerBrokerToController()) {
                    LOGGER.error("Failed to register broker to controller");
                    return false;
                }
                this.registerState = RegisterState.REGISTERED;
                LOGGER.info("Register state change to: {}, masterBrokerId: {}, masterBrokerAddr: {}", this.registerState, this.masterBrokerId, this.masterAddress);
            }
            return true;
        } catch (final Exception e) {
            LOGGER.error("Failed to register broker to controller", e);
            return false;
        }
    }

    private Long getNextBrokerId() {
        try {
            GetNextBrokerIdResponseHeader nextBrokerIdResp = this.brokerOuterAPI.getNextBrokerId(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.controllerLeaderAddress);
            return nextBrokerIdResp.getNextBrokerId();
        } catch (Exception e) {
            LOGGER.error("fail to get next broker id from controller", e);
            return null;
        }
    }

    private boolean createTempMetadataFile(Long brokerId) {
        String registerCheckCode = this.brokerAddress + ";" + System.currentTimeMillis();
        try {
            this.tempBrokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerId, registerCheckCode);
            return true;
        } catch (Exception e) {
            LOGGER.error("update and persist temp broker metadata file failed", e);
            this.tempBrokerMetadata.clear();
            return false;
        }
    }

    private boolean applyBrokerId() {
        try {
            this.brokerOuterAPI.applyBrokerId(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), tempBrokerMetadata.getBrokerId(), tempBrokerMetadata.getRegisterCheckCode(), this.controllerLeaderAddress);
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to apply broker id: {}", e, tempBrokerMetadata.getBrokerId());
            return false;
        }
    }

    private boolean createMetadataFileAndDeleteTemp() {
        try {
            this.brokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), tempBrokerMetadata.getBrokerId());
            this.tempBrokerMetadata.clear();
            this.brokerControllerId = this.brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to create metadata file", e);
            this.brokerMetadata.clear();
            return false;
        }
    }

    private boolean registerBrokerToController() {
        try {
            Pair<RegisterBrokerToControllerResponseHeader, Set<Long>> responsePair = this.brokerOuterAPI.registerBrokerToController(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerControllerId, brokerAddress, controllerLeaderAddress);
            if (responsePair == null)
                return false;
            RegisterBrokerToControllerResponseHeader response = responsePair.getObject1();
            Set<Long> syncStateSet = responsePair.getObject2();
            final Long masterBrokerId = response.getMasterBrokerId();
            final String masterAddress = response.getMasterAddress();
            if (masterBrokerId == null) {
                return true;
            }
            if (this.brokerControllerId.equals(masterBrokerId)) {
                changeToMaster(response.getMasterEpoch(), response.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, response.getMasterEpoch(), masterBrokerId);
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to send registerBrokerToController request to controller", e);
            return false;
        }
    }

    private void confirmNowRegisteringState() {
        try {
            this.brokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read metadata file failed", e);
        }
        if (this.brokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
            this.brokerControllerId = brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return;
        }
        try {
            this.tempBrokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read temp metadata file failed", e);
        }
        if (this.tempBrokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
        }
    }

    private boolean checkMetadataValid() {
        if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
            if (this.tempBrokerMetadata.getClusterName() == null || !this.tempBrokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker temp metadata is different from the clusterName: {} in broker config", this.tempBrokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.tempBrokerMetadata.getBrokerName() == null || !this.tempBrokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker temp metadata is different from the brokerName: {} in broker config", this.tempBrokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
            if (this.brokerMetadata.getClusterName() == null || !this.brokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker metadata is different from the clusterName: {} in broker config", this.brokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.brokerMetadata.getBrokerName() == null || !this.brokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker metadata is different from the brokerName: {} in broker config", this.brokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        return true;
    }

    private void schedulingSyncBrokerMetadata() {
        this.scheduledService.scheduleAtFixedRate(() -> {
            try {
                final Pair<GetReplicaInfoResponseHeader, SyncStateSet> result = this.brokerOuterAPI.getReplicaInfo(this.controllerLeaderAddress, this.brokerConfig.getBrokerName());
                final GetReplicaInfoResponseHeader info = result.getObject1();
                final SyncStateSet syncStateSet = result.getObject2();
                final String newMasterAddress = info.getMasterAddress();
                final int newMasterEpoch = info.getMasterEpoch();
                final Long masterBrokerId = info.getMasterBrokerId();
                synchronized (this) {
                    if (newMasterEpoch > this.masterEpoch) {
                        if (StringUtils.isNoneEmpty(newMasterAddress) && masterBrokerId != null) {
                            if (masterBrokerId.equals(this.brokerControllerId)) {
                                changeToMaster(newMasterEpoch, syncStateSet.getSyncStateSetEpoch(), syncStateSet.getSyncStateSet());
                            } else {
                                changeToSlave(newMasterAddress, newMasterEpoch, masterBrokerId);
                            }
                        } else {
                            brokerElect();
                        }
                    } else if (newMasterEpoch == this.masterEpoch) {
                        if (isMasterState()) {
                            changeSyncStateSet(syncStateSet.getSyncStateSet(), syncStateSet.getSyncStateSetEpoch());
                        }
                    }
                }
            } catch (final MQBrokerException exception) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), exception);
                if (exception.getResponseCode() == CONTROLLER_BROKER_METADATA_NOT_EXIST) {
                    try {
                        registerBrokerToController();
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ignore) {

                    }
                }
            } catch (final Exception e) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), e);
            }
        }, 3 * 1000, this.brokerConfig.getSyncBrokerMetadataPeriod(), TimeUnit.MILLISECONDS);
    }

    private boolean schedulingSyncControllerMetadata() {
        int tryTimes = 0;
        while (tryTimes < 3) {
            boolean flag = updateControllerMetadata();
            if (flag) {
                this.scheduledService.scheduleAtFixedRate(this::updateControllerMetadata, 1000 * 3, this.brokerConfig.getSyncControllerMetadataPeriod(), TimeUnit.MILLISECONDS);
                return true;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignore) {

            }
            tryTimes++;
        }
        LOGGER.error("Failed to init controller metadata, maybe the controllers in {} is not available", this.controllerAddresses);
        return false;
    }

    private boolean updateControllerMetadata() {
        for (String address : this.availableControllerAddresses.keySet()) {
            try {
                final GetMetaDataResponseHeader responseHeader = this.brokerOuterAPI.getControllerMetaData(address);
                if (responseHeader != null && StringUtils.isNoneEmpty(responseHeader.getControllerLeaderAddress())) {
                    this.controllerLeaderAddress = responseHeader.getControllerLeaderAddress();
                    LOGGER.info("Update controller leader address to {}", this.controllerLeaderAddress);
                    return true;
                }
            } catch (final Exception e) {
                LOGGER.error("Failed to update controller metadata", e);
            }
        }
        return false;
    }

    private void schedulingCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
        this.checkSyncStateSetTaskFuture = this.scheduledService.scheduleAtFixedRate(this::checkSyncStateSetAndDoReport, 3 * 1000, this.brokerConfig.getCheckSyncStateSetPeriod(), TimeUnit.MILLISECONDS);
    }

    private void checkSyncStateSetAndDoReport() {
        try {
            final Set<Long> newSyncStateSet = this.haService.maybeShrinkSyncStateSet();
            newSyncStateSet.add(this.brokerControllerId);
            synchronized (this) {
                if (this.syncStateSet != null) {
                    if (this.syncStateSet.size() == newSyncStateSet.size() && this.syncStateSet.containsAll(newSyncStateSet)) {
                        return;
                    }
                }
            }
            doReportSyncStateSetChanged(newSyncStateSet);
        } catch (Exception e) {
            LOGGER.error("Check syncStateSet error", e);
        }
    }

    private void doReportSyncStateSetChanged(Set<Long> newSyncStateSet) {
        try {
            final SyncStateSet result = this.brokerOuterAPI.alterSyncStateSet(this.controllerLeaderAddress, this.brokerConfig.getBrokerName(), this.brokerControllerId, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
            if (result != null) {
                changeSyncStateSet(result.getSyncStateSet(), result.getSyncStateSetEpoch());
            }
        } catch (final Exception e) {
            LOGGER.error("Error happen when change SyncStateSet, broker:{}, masterAddress:{}, masterEpoch:{}, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}", this.brokerConfig.getBrokerName(), this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
        }
    }

    private void stopCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
    }

    private void scanAvailableControllerAddresses() {
        if (controllerAddresses == null) {
            LOGGER.warn("scanAvailableControllerAddresses addresses of controller is null!");
            return;
        }
        for (String address : availableControllerAddresses.keySet()) {
            if (!controllerAddresses.contains(address)) {
                LOGGER.warn("scanAvailableControllerAddresses remove invalid address {}", address);
                availableControllerAddresses.remove(address);
            }
        }
        for (String address : controllerAddresses) {
            scanExecutor.submit(() -> {
                if (brokerOuterAPI.checkAddressReachable(address)) {
                    availableControllerAddresses.putIfAbsent(address, true);
                } else {
                    Boolean value = availableControllerAddresses.remove(address);
                    if (value != null) {
                        LOGGER.warn("scanAvailableControllerAddresses remove unconnected address {}", address);
                    }
                }
            });
        }
    }

    private void updateControllerAddr() {
        if (brokerConfig.isFetchControllerAddrByDnsLookup()) {
            List<String> adders = brokerOuterAPI.dnsLookupAddressByDomain(this.brokerConfig.getControllerAddr());
            if (CollectionUtils.isNotEmpty(adders)) {
                this.controllerAddresses = adders;
            }
        } else {
            final String controllerPaths = this.brokerConfig.getControllerAddr();
            final String[] controllers = controllerPaths.split(";");
            assert controllers.length > 0;
            this.controllerAddresses = Arrays.asList(controllers);
        }
    }

    public int getLastEpoch() {
        return this.haService.getLastEpoch();
    }

    public BrokerRole getBrokerRole() {
        return this.brokerController.getMessageStoreConfig().getBrokerRole();
    }

    public boolean isMasterState() {
        return getBrokerRole() == BrokerRole.SYNC_MASTER;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.haService.getEpochEntries();
    }

    public List<String> getAvailableControllerAddresses() {
        return new ArrayList<>(availableControllerAddresses.keySet());
    }

    public void setFenced(boolean fenced) {
        this.brokerController.setIsolated(fenced);
        this.brokerController.getMessageStore().getRunningFlags().makeFenced(fenced);
    }

    enum State {
        INITIAL,
        FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE,
        REGISTER_TO_CONTROLLER_DONE,
        RUNNING,
        SHUTDOWN,
    }

    enum RegisterState {
        INITIAL,
        CREATE_TEMP_METADATA_FILE_DONE,
        CREATE_METADATA_FILE_DONE,
        REGISTERED
    }

}