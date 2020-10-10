package com.quickstart.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
@ToString
public class JdbcConfig {
    private String driver;
    private boolean open;
    private String name;
    private String url;
    private String user;
    private String passwd;
    private int showCount = 20;
}
