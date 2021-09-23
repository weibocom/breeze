
#[cfg(test)]
mod tests {
    use ds::Slice;

    #[test]
    fn test_split_slice() {
        println!("begin");
        let data = "VALUE key1 0 10\r\nsksksksksk\r\nVALUE key2 0 14\r\nababababababab\r\nEND\r\n";
        let slice = Slice::from(data.as_ref());
        println!("slice generated");
        let split = slice.split("\r\n".as_ref());
        println!("slice split, size = {}", split.len());
        for single in split {
            println!("single = {}", String::from_utf8(Vec::from(single.data())).unwrap());
        }
    }
}
