[English](../../community/contribution-code.md)|[中文](./contribution-code.md)

# 贡献代码

我们非常欢迎贡献代码。

如果需要提交的代码比较多，可以先和我们谈谈！记录一条[Issues]。这对双方达成共识是有帮助的。

Fork QSQL GitHub 库，并为您的提交创建一个分支。

开发您的代码和测试用例，运行测试用例来验证您的修改是正确的。

提交代码到您的分支上。

如果您的更改有多个提交，请使用 `git rebase -i master` 将它们压缩为单个提交，并将代码更新到主线上的最新版本。

然后将您的提交推送到 GitHub 上，并从您的分支创建一个 pull 请求到 QSQL 主分支，committer 将会检查您的更改。

在提交之后，pull request 可能需要更新，原因如下：

- 您在提交 pull request 之后发现了一个问题
- reviewer 要求进一步修改

为了更新 pull 请求，需要在分支中提交更改，然后将提交推到 GitHub。我们鼓励您在现有提交的基础上使用常规（非基于重新构建）提交。

当将更改推送到 GitHub 时，您应该避免使用 `--force` 参数及其替代方法。您可以选择在某些条件下强制推行您的更改：

- 最近一次的 pull request 的提交在10分钟之内，并且没有关于它的未决讨论
- reviewer明确要求您执行一些需要使用 `--force` 选项的修改