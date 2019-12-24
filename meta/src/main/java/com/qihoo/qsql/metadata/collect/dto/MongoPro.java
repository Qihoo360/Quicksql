package com.qihoo.qsql.metadata.collect.dto;

import javax.validation.constraints.NotNull;

public class MongoPro {
    @NotNull
    private String host;
    @NotNull
    private Integer port;

    private String database;

    private String authMechanism;

    private String username;

    private String password;
}
