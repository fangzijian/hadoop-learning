package com.fzj.flink.learning.domain;

public class UserAction {

    private String userId;
    private long date;
    private String action;
    private String produce;
    private long time;


    public UserAction() {
    }

    public UserAction(String userId, long date, String action, String produce, long time) {
        this.userId = userId;
        this.date = date;
        this.action = action;
        this.produce = produce;
        this.time = time;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getProduce() {
        return produce;
    }

    public void setProduce(String produce) {
        this.produce = produce;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", date=" + date +
                ", action='" + action + '\'' +
                ", produce='" + produce + '\'' +
                ", time=" + time +
                '}';
    }
}
