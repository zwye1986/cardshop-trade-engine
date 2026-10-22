-- 仅供新建可丢弃schema的最小旧表前置结构，不是发布DDL或完整旧库快照。
-- 引擎结构必须由真实master changelog发布；参见 docs/交行直充业务系统详细设计.md 9.8。
CREATE TABLE mc_order (
    id bigint NOT NULL AUTO_INCREMENT UNIQUE COMMENT '表主键',
    order_Id varchar(100) NOT NULL COMMENT '平台订单号',
    org_code varchar(64) DEFAULT NULL COMMENT '商户编码',
    out_orderId varchar(100) DEFAULT NULL COMMENT '商户订单号',
    product_type int DEFAULT NULL COMMENT '商品类型',
    order_status varchar(8) DEFAULT NULL,
    num bigint DEFAULT NULL,
    produt_code varchar(64) DEFAULT NULL,
    supplier_channel varchar(64) DEFAULT NULL,
    account varchar(32) DEFAULT NULL,
    data longtext,
    data_status varchar(32) DEFAULT NULL,
    remark varchar(500) DEFAULT NULL,
    flow_id bigint DEFAULT NULL,
    edit_time datetime DEFAULT NULL,
    create_time datetime DEFAULT NULL,
    end_time datetime DEFAULT NULL,
    product_name varchar(255),
    product_price bigint,
    product_value bigint,
    out_order_no varchar(100),
    order_price bigint,
    refund_order_price bigint DEFAULT 0,
    callback_url varchar(500),
    request_ip varchar(64),
    res_data longtext,
    parent_id varchar(100),
    coupon_type varchar(32),
    order_type varchar(32),
    PRIMARY KEY (order_Id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Task10生产凭据事务的最小旧表投影；没有额外唯一券约束替代生产guard协议。
CREATE TABLE mc_product (
    product_id varchar(64) NOT NULL PRIMARY KEY,
    quantity bigint DEFAULT NULL,
    enable_quantity bigint DEFAULT NULL,
    external_product_id varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mc_batch_record (
    id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_no varchar(100) DEFAULT NULL,
    batch_id bigint DEFAULT NULL,
    coupon_code varchar(128) COLLATE utf8mb4_bin DEFAULT NULL,
    begin_time datetime DEFAULT NULL,
    end_time datetime DEFAULT NULL,
    phone varchar(32) DEFAULT NULL,
    state_type varchar(8) DEFAULT NULL,
    state varchar(8) DEFAULT NULL,
    channel varchar(32) DEFAULT NULL,
    create_time datetime DEFAULT NULL,
    invalid_time datetime DEFAULT NULL,
    KEY idx_fixture_batch_order (order_no, channel, state_type, id),
    KEY idx_fixture_batch_coupon (coupon_code, channel, state_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mc_use_history (
    id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    coupon_code varchar(128) COLLATE utf8mb4_bin DEFAULT NULL,
    use_time datetime DEFAULT NULL,
    channel varchar(32) DEFAULT NULL,
    create_time datetime DEFAULT NULL,
    KEY idx_fixture_history_coupon (coupon_code, channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mc_order_supplier_flow (
    id bigint NOT NULL PRIMARY KEY,
    order_id varchar(100) DEFAULT NULL,
    status varchar(32) DEFAULT NULL,
    remark varchar(500) DEFAULT NULL,
    update_time datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mc_depart_record (
    id bigint NOT NULL AUTO_INCREMENT COMMENT '表主键',
    order_id varchar(100) DEFAULT NULL COMMENT '平台订单号',
    type varchar(8) DEFAULT NULL COMMENT '流水类型',
    org_code varchar(64),
    value bigint,
    create_time datetime,
    before_balance decimal(20, 2) DEFAULT NULL COMMENT '变动前余额',
    balance decimal(20, 2) DEFAULT NULL COMMENT '变动后余额',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Task11退款生产事务所需旧表投影；只用于本次随机schema，不是发布迁移。
CREATE TABLE mc_ext_depart (
    org_code varchar(64) PRIMARY KEY,
    balance bigint,
    credit_status varchar(8),
    aes_key varchar(128),
    depart_status int DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE mc_redpacket_record (
    id bigint AUTO_INCREMENT PRIMARY KEY,
    order_id varchar(100),
    request_channel varchar(64),
    channel varchar(64),
    state varchar(8),
    value bigint
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Task15旧通知配置的最小投影，仅供随机本地schema；不作为生产DDL。
CREATE TABLE mc_config (
    org_code varchar(64),
    config_key varchar(64),
    config_value varchar(1024)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
