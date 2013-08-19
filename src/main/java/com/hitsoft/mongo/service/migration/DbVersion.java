package com.hitsoft.mongo.service.migration;

import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.repository.Managed;

/**
 * User: smeagol
 * Date: 26.12.10
 * Time: 23:16
 */
public class DbVersion extends ManagedObject {

  public static enum Field {
    VERSION
  }

  public String version;

  public DbVersion() {
  }

  public Update update() {
    return new Update();
  }

  public class Update {
    public void setVersion(String version) {
      DbVersion.this.version = version;
      updateFields(Field.VERSION);
    }
  }

  public static class Query {
    private static Managed.QueryBuilder<DbVersion> q() {
      return Managed.query(DbVersion.class);
    }

    public static Managed.QueryBuilder<DbVersion>.Exec<DbVersion> all() {
      return q().exec;
    }
  }

}
