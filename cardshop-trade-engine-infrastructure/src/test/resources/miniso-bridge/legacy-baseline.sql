-- 仅供新建可丢弃schema的最小旧表前置结构，不是发布DDL或完整旧库快照。
-- 引擎结构必须由真实master changelog发布；参见 docs/交行直充业务系统详细设计.md 9.8。
CREATE TABLE mc_order (
    order_Id varchar(100) NOT NULL COMMENT '平台订单号',
    org_code varchar(64) DEFAULT NULL COMMENT '商户编码',
    out_orderId varchar(100) DEFAULT NULL COMMENT '商户订单号',
    product_type int DEFAULT NULL COMMENT '商品类型',
    PRIMARY KEY (order_Id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mc_depart_record (
    id bigint NOT NULL AUTO_INCREMENT COMMENT '表主键',
    order_id varchar(100) DEFAULT NULL COMMENT '平台订单号',
    type varchar(8) DEFAULT NULL COMMENT '流水类型',
    before_balance decimal(20, 2) DEFAULT NULL COMMENT '变动前余额',
    balance decimal(20, 2) DEFAULT NULL COMMENT '变动后余额',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
