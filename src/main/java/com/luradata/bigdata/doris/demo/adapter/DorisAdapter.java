package com.luradata.bigdata.doris.demo.adapter;

import java.io.Closeable;
import java.sql.Connection;

public interface DorisAdapter extends Closeable {

    Connection getConnection();

}
