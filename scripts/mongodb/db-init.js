db.createUser(
  {
    user: "dbuser",
    pwd: "dbpasswd",
    roles: [ { role: "readWrite", db: "dev" }],
    passwordDigestor: "server"
  }
);

db.createUser(
  {
    user: "readonly",
    pwd: "readpasswd",
    roles: [ { role: "read", db: "dev" }],
    passwordDigestor: "server"
  }
);
