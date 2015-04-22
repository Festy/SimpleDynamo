package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by utsavpatel on 4/15/15.
 */


public class Message implements Serializable{
    enum TYPE{
        READ_ONE, WRITE_OWN, WRITE_REPLICA, WRITE, ACK, READ_ALL, REPLY_ALL, REPLY_ONE, DELETE_ONE, DELETE_ALL,DELETE_CORD, GET_ME_ALL, RECEIVE_YOU_ALL;
    }
    private TYPE type;
    private String key, value, keyHash;
    private String senderPort;
    private String remortPort;
    private boolean hasCoordinatorFailed = false;
    private HashMap<String, String> result;
    private int messagID;
    private String coordinator;

    public void setValue(String value) {
        this.value = value;
    }

    public void setMessagID(int messagID) {
        this.messagID = messagID;
    }

    public String getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(String coordinator) {
        this.coordinator = coordinator;
    }

    public int getMessagID() {
        return messagID;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setRemortPort(String remortPort) {
        this.remortPort = remortPort;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

    public void setCoordinatorFailure(boolean coordinatorFailure) {
        this.hasCoordinatorFailed = coordinatorFailure;
    }

    public boolean getCoordinatorFailure() {
        return hasCoordinatorFailed;
    }


    public String getRemortPort() {
        return remortPort;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public void setKeyHash(String keyHash) {
        this.keyHash = keyHash;
    }

    public String getKeyHash() {
        return keyHash;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public TYPE getType() {
        return type;
    }

    public void setResult(HashMap<String, String> result) {
        this.result = result;
    }

    public HashMap<String, String> getResult() {
        return result;
    }
}
