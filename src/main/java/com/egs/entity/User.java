package com.egs.entity;

public class User {
    String userCode;
    int userId;

    public User(String userCode, int userId) {
        this.userCode = userCode;
        this.userId = userId;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }
}
