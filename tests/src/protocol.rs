#[cfg(test)]
mod proc_test {
    use ds::RingSlice;
    use protocol::Bit;
    use protocol::Packet;

    #[test]
    fn parse_rsp() {
        let data: RingSlice = SIMPLE_ARR.as_bytes().into();
        let data: Packet = data.into();
        let mut oft: usize = 0;
        assert!(data.num_skip_all(&mut oft).is_ok());
        assert_eq!(oft, data.len());

        let mut oft: usize = 0;
        assert!(data.skip_all_bulk(&mut oft).is_ok());
        assert_eq!(oft, data.len());
    }

    #[test]
    fn parse_command_rsp() {
        let rsp = "*2\r\n*6\r\n$5\r\nhost:\r\n:-1\r\n*2\r\n+loading\r\n+stale\r\n:0\r\n:0\r\n:0\r\n*6\r\n$11\r\nunsubscribe\r\n:-1\r\n*4\r\n+pubsub\r\n+noscript\r\n+loading\r\n+stale\r\n:0\r\n:0\r\n:0\r\n";
        let rsp_data: RingSlice = rsp.as_bytes().into();
        let rsp_data: Packet = rsp_data.into();
        let mut oft: usize = 0;
        assert!(rsp_data.num_skip_all(&mut oft).is_ok());
        assert_eq!(oft, rsp_data.len());

        let mut oft: usize = 0;
        assert!(rsp_data.skip_all_bulk(&mut oft).is_ok());
        assert_eq!(oft, rsp_data.len());
    }

    #[test]
    fn bit() {
        let mut v: u64 = 0;
        //设置第0，3位
        v.set(0);
        v.set(2);
        assert_eq!(v, 5);
        assert_eq!(v.get(2), true);
        assert_eq!(v.get(1), false);

        //设置567位
        v.mask_set(4, 0xf, 0x7);
        assert_eq!(v, 0x75);
        assert_eq!(v.mask_get(4, 0xf), 0x7);

        v.clear(0);
        v.clear(2);
        assert_eq!(v, 0x70);
    }
    // 65个str类型的数组
    static SIMPLE_ARR:&'static str = "*65\r\n$16\r\nad_61fa9e8bc3624\r\n$169\r\n{\"adid\":\"ad_61fa9e8bc3624\",\"start\":1643817600,\"end\":1646496000,\"exposure\":100000000,\"mids\":\"4731641022647893,4731670442019006\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_62024d35f2de0\r\n$169\r\n{\"adid\":\"ad_62024d35f2de0\",\"start\":1644336000,\"end\":1647014400,\"exposure\":100000000,\"mids\":\"4733663557980874,4734162701387024\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61f7666e70f64\r\n$169\r\n{\"adid\":\"ad_61f7666e70f64\",\"start\":1643558400,\"end\":1646236800,\"exposure\":100000000,\"mids\":\"4728771901133069,4731640230183405\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_620370eed4001\r\n$169\r\n{\"adid\":\"ad_620370eed4001\",\"start\":1644336000,\"end\":1647014400,\"exposure\":100000000,\"mids\":\"4730337131237645,4734730736506689\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$13\r\n0000000000000\r\n$155\r\n{\"adid\":\"0000000000000\",\"start\":1,\"end\":1641394800000,\"exposure\":2147483647,\"mids\":\"\",\"uid_blacklist\":[],\"keyword_blacklist\":[\"诺贝尔_梁特别纪念\"]}\r\n$6\r\n888888\r\n$118\r\n{\"adid\":\"888888\",\"start\":1641952859,\"end\":1636732800,\"exposure\":0,\"mids\":\"\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61fa9fecafbb0\r\n$169\r\n{\"adid\":\"ad_61fa9fecafbb0\",\"start\":1643817600,\"end\":1646496000,\"exposure\":100000000,\"mids\":\"4731641022647893,4731670442019006\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_621cca647edc7\r\n$169\r\n{\"adid\":\"ad_621cca647edc7\",\"start\":1645977600,\"end\":1648569600,\"exposure\":100000000,\"mids\":\"4722181898240555,4722187947212819\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61ef5e9ca6a58\r\n$128\r\n{\"adid\":\"ad_61ef5e9ca6a58\",\"start\":1643077260,\"end\":1643595661,\"exposure\":0,\"mids\":\"\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_6206317004e93\r\n$169\r\n{\"adid\":\"ad_6206317004e93\",\"start\":1644508800,\"end\":1647187200,\"exposure\":100000000,\"mids\":\"4734886621480080,4735615411424926\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$7\r\nlj_test\r\n$69\r\n{\"adid\":\"lj_test\",\"start\":1547811967,\"end\":4476654671,\"exposure\":100}\r\n$16\r\nad_621c8cf3ed4da\r\n$169\r\n{\"adid\":\"ad_621c8cf3ed4da\",\"start\":1646100000,\"end\":1648742400,\"exposure\":100000000,\"mids\":\"4736804262248952,4736733373533937\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_6200ced6323a5\r\n$169\r\n{\"adid\":\"ad_6200ced6323a5\",\"start\":1644163200,\"end\":1646841600,\"exposure\":100000000,\"mids\":\"4726598756205880,4732063011572467\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_621caee0a2410\r\n$169\r\n{\"adid\":\"ad_621caee0a2410\",\"start\":1646100000,\"end\":1648742400,\"exposure\":100000000,\"mids\":\"4736804262248952,4736733373533937\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_621625d65d2eb\r\n$169\r\n{\"adid\":\"ad_621625d65d2eb\",\"start\":1645545600,\"end\":1648224000,\"exposure\":100000000,\"mids\":\"4734972374549029,4737921637421709\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_62063250bd30a\r\n$169\r\n{\"adid\":\"ad_62063250bd30a\",\"start\":1644508800,\"end\":1647187200,\"exposure\":100000000,\"mids\":\"4734886621480080,4735615411424926\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_62024c68a9273\r\n$169\r\n{\"adid\":\"ad_62024c68a9273\",\"start\":1644336000,\"end\":1647014400,\"exposure\":100000000,\"mids\":\"4733663557980874,4734162701387024\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_621cbf813a42a\r\n$169\r\n{\"adid\":\"ad_621cbf813a42a\",\"start\":1645977600,\"end\":1648656000,\"exposure\":100000000,\"mids\":\"4741545502441988,4741580051712413\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61f794caa1369\r\n$169\r\n{\"adid\":\"ad_61f794caa1369\",\"start\":1643558400,\"end\":1646236800,\"exposure\":100000000,\"mids\":\"4730930545562652,4731646026973271\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nwjtestmml8888888\r\n$134\r\n{\"adid\":\"wjtestmml8888888\",\"start\":1,\"end\":9223372036854775807,\"exposure\":1000000,\"mids\":\"\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_6214a5d19802b\r\n$169\r\n{\"adid\":\"ad_6214a5d19802b\",\"start\":1645545600,\"end\":1648224000,\"exposure\":100000000,\"mids\":\"4734972374549029,4737921637421709\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_6131ee328227d\r\n$95\r\n{\"adid\":\"ad_6131ee328227d\",\"start\":1630598400,\"end\":1633017600,\"exposure\":2000000000,\"mids\":\"\"}\r\n$16\r\nad_620a05bed5bd2\r\n$152\r\n{\"adid\":\"ad_620a05bed5bd2\",\"start\":1644768000,\"end\":1647446400,\"exposure\":100000000,\"mids\":\"4736726230895209\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$17\r\ndaoguang3_test_12\r\n$105\r\n{\"adid\":\"daoguang3_test_12\",\"start\":1575861384,\"end\":2207877384,\"exposure\":222,\"mids\":\"4468286953166651\"}\r\n$16\r\nad_620a04c645bc3\r\n$152\r\n{\"adid\":\"ad_620a04c645bc3\",\"start\":1644768000,\"end\":1647446400,\"exposure\":100000000,\"mids\":\"4736726230895209\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_612c96944b18c\r\n$95\r\n{\"adid\":\"ad_612c96944b18c\",\"start\":1630598400,\"end\":1633017600,\"exposure\":2000000000,\"mids\":\"\"}\r\n$16\r\nad_619c94c9e1a3f\r\n$95\r\n{\"adid\":\"ad_619c94c9e1a3f\",\"start\":1637651606,\"end\":1648710807,\"exposure\":2100000000,\"mids\":\"\"}\r\n$16\r\nad_621cc078b14e5\r\n$169\r\n{\"adid\":\"ad_621cc078b14e5\",\"start\":1645977600,\"end\":1648656000,\"exposure\":100000000,\"mids\":\"4741545502441988,4741580051712413\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61f765cf92f4b\r\n$169\r\n{\"adid\":\"ad_61f765cf92f4b\",\"start\":1643558460,\"end\":1646236800,\"exposure\":100000000,\"mids\":\"4728771901133069,4731640230183405\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_61f79446ac00f\r\n$169\r\n{\"adid\":\"ad_61f79446ac00f\",\"start\":1643558400,\"end\":1646236800,\"exposure\":100000000,\"mids\":\"4730930545562652,4731646026973271\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_6200d040e9c05\r\n$169\r\n{\"adid\":\"ad_6200d040e9c05\",\"start\":1644163200,\"end\":1646841600,\"exposure\":100000000,\"mids\":\"4726598756205880,4732063011572467\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_62036e6a0a0b1\r\n$169\r\n{\"adid\":\"ad_62036e6a0a0b1\",\"start\":1644336000,\"end\":1647014400,\"exposure\":100000000,\"mids\":\"4730337131237645,4734730736506689\",\"uid_blacklist\":[],\"keyword_blacklist\":[]}\r\n$16\r\nad_testbychange3\r\n";
}
