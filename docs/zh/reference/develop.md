## QuickSQL开发

(1)  第一步找到QuickSQL项目 https://github.com/Qihoo360/Quicksql 

![image-20191230004559769](../images/develop/image-20191230004559769.png)

（2）第二步down到本地idea

<img src="../images/develop/image-20191230115105541.png" alt="image-20191230115105541" style="zoom:150%;" />

(3) 第三步本地编译及打包

![image-20191230091435673](../images/develop/image-20191230091435673.png)

（4）第四步本地运行example

![image-20191230091732159](../images/develop/image-20191230091732159.png)

ps:混合查询测试

由于本地测试需要使用spark相关环境，需要修改父pom下spark的scope 为compile形式

(5) 第五步修改pom

![image-20191231172639084](../images/develop/image-20191231172639084.png)

（6）第六步运行混查example

![image-20191231172837518](../images/develop/image-20191231172837518.png)

