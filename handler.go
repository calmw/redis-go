package main

import "sync"

/// 命令处理
/// 我们从客户端收到的请求将是一个 RESP 数组，它告诉我们它想要发送哪个命令;因此，我们总是根据命令名称定义处理程序，它是 RESP 数组中的第一个元素，其余元素将是参数

// Handlers
// 请注意，我们用大写字母书写命令名称，因为 Redis 命令不区分大小写。
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var SETs = map[string]string{}

// SETsMu 我们使用sync.RWMutex是因为我们的服务器需要并发处理请求。我们使用 RWMutex 来确保 SET 映射不会被多个线程同时修改
var SETsMu = sync.RWMutex{}

// HSETs 代码与 SET 和 GET 命令非常相似。不同之处在于 HSET 将是map[string]map[string]string。
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

// 命令 PING
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

// 我们在这里所做的是接收第一个请求并从中提取 Value 对象。然后，我们执行了一些验证，以确保命令是一个数组并且不为空。之后，我们取出数组中的第一个元素并将其转换为大写，这将是命令名称。其余元素将是参数。
// 如果您对这个解释不清楚，这里有一个当我们说 SET name Ahmed 时的 Value 对象的例子：
//
//	Value{
//		 typ: "array",
//		 array: []Value{
//		 	Value{typ: "bulk", bulk: "SET"},
//		 	Value{typ: "bulk", bulk: "name"},
//		 	Value{typ: "bulk", bulk: "Ahmed"},
//		 },
//	}
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// 你大概可以想象 GET 命令是如何工作的。如果我们找到了键，我们就返回它的值；否则，我们返回 nil
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// HSET 和 HGET 命令
// 简单来说，这些命令是哈希映射中的哈希映射。它是一个map[string]map[string]string。它采用哈希的名称，后跟键和值。这使我们能够存储如下数据：
// {
//	 "users": {
//		"u1": "Ahmed",
//		"u2": "Mohamed",
//	 },
//	 "posts": {
//		"p1": "Hello World",
//		"p2": "Welcome to my blog",
//	 },
// }

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}
