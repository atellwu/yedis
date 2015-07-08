package com.yeahmobi.yedis.group;

public enum ReadMode {
    /**
     * Master：写操作和读操作都访问 master。
     */
    MASTER,
    /**
     * MasterPreferred：写操作访问 master；读操作访问 master，但当 master 不可用时，则访问 slave。
     */
    MASTERPREFERRED,
    /**
     * Slave：写操作访问 master；读操作访问 slave，slave 不可用时读调用会报错。
     */
    SLAVE,
    /**
     * SlavePreferred：写操作访问 master；读操作访问 slave，但当 slave 都不可用时，则访问 master
     */
    SLAVEPREFERRED;

}
