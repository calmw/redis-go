#### RESP 协议

- 官方文档： https://redis.io/docs/latest/develop/reference/protocol-spec/

#### RESP 示例

```shell
# 下面是一条redis命令
SET admin ahmed
# 我们查看如何SET admin ahmed将其作为序列化消息发送到 Redis，它将如下所示：
*3\r\n$3\r\nset\r\n$5\r\nadmin\r\n$5\r\nahmed
#进一步简化一下：
*3
$3
set
$5
admin
$5
ahmed
# '*' 表示我们有一个大小为 3 的数组。因此，我们将读取 6 行。每对行代表对象的类型和大小，第二行包含该对象的值。
# '$' 表示它是一个长度为 5 的字符串。因此下一行将包含恰好 5 个字符



```