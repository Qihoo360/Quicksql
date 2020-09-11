package com.qihoo.qsql.metadata.collect.dto;

import javax.validation.constraints.NotNull;

public class ElasticsearchProp {
    @NotNull
    private String esNodes;
    @NotNull
    private int esPort;
    @NotNull
    private String esUser;
    @NotNull
    private String esPass;
    @NotNull
    private String esName;

    public String getEsNodes() {
        return esNodes;
    }

    public void setEsNodes(String esNodes) {
        this.esNodes = esNodes;
    }

    public int getEsPort() {
        return esPort;
    }

    public void setEsPort(int esPort) {
        this.esPort = esPort;
    }

    public String getEsUser() {
        return esUser;
    }

    public void setEsUser(String esUser) {
        this.esUser = esUser;
    }

    public String getEsPass() {
        return esPass;
    }

    public void setEsPass(String esPass) {
        this.esPass = esPass;
    }

    public String getEsName() {
        return esName;
    }

    public void setEsName(String esName) {
        this.esName = esName;
    }
}
