[English](../../community/contribution-docs.md)|[中文](./contribution-docs.md)

# **贡献文档**

## 安装 Python
Install [Python](https://www.python.org/) or [Anaconda](https://www.anaconda.com/).

[MkDocs](https://www.mkdocs.org/) 支持的Python版本 2.7, 3.4, 3.5, 3.6, 3.7。

## 安装 pip 
- 如果你本地有 pip 的环境，请升级 pip 版本：

```
pip install --upgrade pip
```

- 如果本地没有 pip 环境，请下载后安装：

```shell
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

## 安装 MkDocs 和其他依赖
用 pip 安装 MkDocs 和其他依赖： 

```shell
pip install mkdocs
pip install mkdocs-material
pip install pygments
pip install pymdown-extensions
```

执行 `mkdocs --version` 确定 MkDocs 安装成功并能运行。

## 写 Markdown 文档
- 在 `/docs` 下创建一个新的 `.md` 文件
- 编辑 `mkdocs.yml` 添加新页面。 [教程](https://www.mkdocs.org/#adding-pages)

```yml
nav:
  - Home: 'index.md'
  - User Guide:
      - 'JDBC Doc': 'user-guide/jdbc.md'
      - 'Build Doc': 'user-guide/build.md'
      - 'API Doc': 'user-guide/api.md'
  - Community:
      - Channel: 'community.md'
      - Contribution:
        - Code: 'contribution-code.md'
        - Docs: 'contribution-docs.md'
  - About:
      - 'License': 'about/license.md'
      - 'Release Notes': 'about/release-notes.md'
```

## 提交

提交所有改动，Push 后在 GitHub 发起一个 PR 请求，ReadtheDocs 将会自动构建。