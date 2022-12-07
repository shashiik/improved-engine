# improved-engine
- The purpose of this utility file is to support basic CRUD operations in our API. 
- The required update to this file is to support a single transaction bulk PUT.

```
[
  {
    "id": 1
  },
  {
    "id": 2
  },
  {
    "id": 3
  },
  {
    "id": 4
  }
]
```

In this PUT call, for each id, several database/api calls are made. For each id, if any of those database calls fail, we want to fail all the db/api calls for that id. This is in order so that the db doesn't contain partially-accurate data. Either it's all right, or it's not updated. Id 1 can pass, id 2 can fail, and id 3 and 4 can pass. Or all can pass, or all can fail.
