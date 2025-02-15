
### posts TABLE 생성 QUERY
```SQL
CREATE TABLE posts (
    post_id VARCHAR(255) NOT NULL,
    post_url VARCHAR(2083) NOT NULL,
    title VARCHAR(500) NOT NULL,
    content VARCHAR(MAX) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    view_count BIGINT,
    upvote_count BIGINT,
    downvote_count BIGINT,
    comment_count BIGINT,

    PRIMARY KEY (post_id)
);
```

### comments TABLE 생성 QUERY
``` SQL
CREATE TABLE comments (
    comment_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255) NOT NULL,
    content VARCHAR(MAX) NOT NULL,
    is_reply BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    upvote_count BIGINT,
    downvote_count BIGINT,

    PRIMARY KEY (comment_id)
);
```

### Redshift COPY 명령어 (in SQL query editor)
```SQL
COPY '<database>'
FROM '<s3 위치>'
IAM_ROLE '<IAM arn>'
FORMAT AS PARQUET;
```

### AWS CLI에서 COPY 명령어 실행하기
1. COPY 명령어 실행
    ```shell
    aws redshift-data execute-statement \
      --workgroup-name <workgroup> \
      --database <database> \
      --secret-arn <secret arn> \
      --sql "<sql_query>" \
      --output json
    ```
2. 실행 상태 확인
   ```shell
   aws redshift-data describe-statement --id <query id>
   ```
