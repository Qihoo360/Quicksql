[English](./contribution-code.md)|[中文](../zh/community/contribution-code.md)

# Contributing Code

We welcome contributions.

If you are interested in Quicksql, you can download the source code from GitHub and execute the following maven command at the project root directory：

```shell
mvn -DskipTests clean package
```

If you are planning to make a large contribution, talk to us first! It helps to agree on the general approach. Log a Issures on GitHub for your proposed feature.

Fork the GitHub repository, and create a branch for your feature.

Develop your feature and test cases, and make sure that `mvn install` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch.

If your change had multiple commits, use `git rebase -i master` to squash them into a single commit, and to bring your code up to date with the latest on the main line.

Then push your commit(s) to GitHub, and create a pull request from your branch to the QSQL master branch. Update the JIRA case to reference your pull request, and a committer will review your changes.

The pull request may need to be updated (after its submission) for two main reasons:

1. you identified a problem after the submission of the pull request;
2. the reviewer requested further changes;

In order to update the pull request, you need to commit the changes in your branch and then push the commit(s) to GitHub. You are encouraged to use regular (non-rebased) commits on top of previously existing ones.
