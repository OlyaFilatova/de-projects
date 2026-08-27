# Using AWS CLI with LocalStack

```sh
curl -fsSL https://awscli.amazonaws.com/v2/install.sh | bash
```

Add to `~/.zshrc` or `~/.bashrc`

```sh
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```


```bash
aws --endpoint-url=http://localhost:4566 \
    s3 ls
```

```sh
aws --endpoint-url=http://localhost:4566 \
    s3 ls s3://osint-raw/raw/
```
