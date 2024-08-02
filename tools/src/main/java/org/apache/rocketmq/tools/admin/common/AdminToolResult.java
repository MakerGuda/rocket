package org.apache.rocketmq.tools.admin.common;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AdminToolResult<T> {

    private boolean success;

    private int code;

    private String errorMsg;

    private T data;

    public AdminToolResult(boolean success, int code, String errorMsg, T data) {
        this.success = success;
        this.code = code;
        this.errorMsg = errorMsg;
        this.data = data;
    }

    public static AdminToolResult success(Object data) {
        return new AdminToolResult(true, AdminToolsResultCodeEnum.SUCCESS.getCode(), "success", data);
    }

    public static AdminToolResult failure(AdminToolsResultCodeEnum errorCodeEnum, String errorMsg) {
        return new AdminToolResult<>(false, errorCodeEnum.getCode(), errorMsg, null);
    }

    public static AdminToolResult failure(AdminToolsResultCodeEnum errorCodeEnum, String errorMsg, Object data) {
        return new AdminToolResult<>(false, errorCodeEnum.getCode(), errorMsg, data);
    }

}