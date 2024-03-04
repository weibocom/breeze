# Breeze

Breeze is an access proxy designed for creating reliable, asynchronous, and straightforward applications for stateful database resource access, written in Rust. It is:

* **Fast**: Developed based on the tokio runtime, Breeze ensures exceptionally low overhead throughout the request handling process.

* **Distributed**: Breeze offers multiple distributed access strategies. The application adapts dynamically to changes in storage resource capacity without the need for upgrades or restarts, enabling real-time perception of topology changes.

* **Reliable**: Breeze leverages Rust's memory safety and concurrency model, and reduces bugs in a multithreaded environment and enhances thread safety.

![CI](https://github.com/weibocom/breeze/actions/workflows/rust.yml/badge.svg?branch=dev)
[![Coverage Status](https://coveralls.io/repos/github/weibocom/breeze/badge.svg?branch=master)](https://coveralls.io/github/weibocom/breeze?branch=dev_ci)

## Overview

Breeze offers multi-protocol support, accommodating mainstream resource access protocols, including Memcached, Redis, and MySQL protocols. The currently supported protocol commands include:

   * Memcached:
      * get
      * multi-get
      * set
      * cas
      * delete
   * Redis:
      * get/mget
      * set/mset
      * zset
      * range/zrange
   * MySQL:
      * Backend support for MySQL protocol, frontend support is not available.
      
## Example

Once Breeze is started, you can use any community Memcached SDK to access Breeze.
Here's an example of accessing backend resources using the Memcached (mc) protocol through Breeze. Here is a example from [mecache](https://docs.rs/memcache/latest/memcache/)


``` rust
// create connection with to memcached server node:
let client = memcache::connect("memcache://127.0.0.1:12345?timeout=10&tcp_nodelay=true").unwrap();

// flush the database:
client.flush().unwrap();

// set a string value:
client.set("foo", "bar", 0).unwrap();

// retrieve from memcached:
let value: Option<String> = client.get("foo").unwrap();
assert_eq!(value, Some(String::from("bar")));
assert_eq!(value.unwrap(), "bar");

// prepend, append:
client.prepend("foo", "foo").unwrap();
client.append("foo", "baz").unwrap();
let value: String = client.get("foo").unwrap().unwrap();
assert_eq!(value, "foobarbaz");

// delete value:
client.delete("foo").unwrap();

// using counter:
client.set("counter", 40, 0).unwrap();
client.increment("counter", 2).unwrap();
let answer: i32 = client.get("counter").unwrap().unwrap();
assert_eq!(answer, 42);

```