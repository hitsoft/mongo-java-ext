package com.hitsoft.mongo.service.migration;

import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.managed.ObjectField;
import com.hitsoft.mongo.repository.Managed;

import java.util.Date;

/**
 * User: smeagol
 * Date: 26.12.10
 * Time: 23:16
 */
public class DbVersion extends ManagedObject {

	public static enum Field {
		VERSION,
		LOCK
	}

	public String version;

	@ObjectField(type = Lock.class)
	public Lock lock;

	public static class Lock {
		public String comment;
		public Date date;
	}

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

		public boolean lock(String comment) {
			if (DbVersion.this.lock == null) {
				DbVersion.this.lock = new Lock();
				DbVersion.this.lock.date = new Date();
				DbVersion.this.lock.comment = comment;
				updateFields(Field.LOCK);
				return true;
			} else {
				return false;
			}
		}

		public void unlock() {
			if (DbVersion.this.lock != null) {
				DbVersion.this.lock = null;
				updateFields(Field.LOCK);
			}
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
