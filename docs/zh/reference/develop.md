[English](../../reference/develop.md)|[中文](./develop.md)

# QuickSQL 开发

(1)  第一步找到 QuickSQL 项目 https://github.com/Qihoo360/Quicksql 

![image-20191230004559769](../../images/develop/image-20191230004559769.png)

（2）第二步 down 到本地 idea

![image-20191230115105541](../../images/develop/image-20191230115105541.png)

(3) 第三步本地编译及打包

![image-20191230091435673](../../images/develop/image-20191230091435673.png)

（4）第四步本地运行 example

![image-20191230091732159](../../images/develop/image-20191230091732159.png)

ps:混合查询测试

由于本地测试需要使用 spark 相关环境，需要修改父 pom 下 spark 的 scope 为 compile 形式

(5) 第五步修改 pom

![image-20191231172639084](../../images/develop/image-20191231172639084.png)

（6）第六步运行混查 example

![image-20191231172837518](../../images/develop/image-20191231172837518.png)