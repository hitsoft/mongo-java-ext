package com.hitsoft.mongo.managed;

/**
 * User: Maxim S. Ivanov
 * Date: 22.12.10
 * Time: 0:37
 */

import com.hitsoft.mongo.basic.FieldName;
import com.hitsoft.mongo.basic.UpdateBuilder;
import com.hitsoft.mongo.repository.Managed;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base object for all entities which will be stored in DB automatically
 * All children MUST implement constructor with no arguments it's required for reflection API
 */
public class ManagedObject {

  private static class Log {
    public static final Logger LOG = LoggerFactory.getLogger(ManagedObject.class);
  }

  public ObjectId _id;

  public ManagedObject() {
  }

  protected ManagedObject(ObjectId _id) {
    this._id = _id;
  }

  public String getCollectionName() {
    return getCollectionName(getClass());
  }

  public static String getCollectionName(Class<?> clazz) {
    return clazz.getSimpleName();
  }

  protected Object getValueByField(Enum field) {
    Object result = null;
    try {
      result = this.getClass().getField(FieldName.get(field)).get(this);
    } catch (IllegalAccessException e) {
      Log.LOG.error("", e);
    } catch (NoSuchFieldException e) {
      Log.LOG.error("", e);
    }
    return result;
  }

  public interface FieldProcessor {
    void process(Class clazz, java.lang.reflect.Field field, String fieldName, Object fieldValue);
  }

  public void iterateFields(FieldProcessor processor) {
    Class clazz = this.getClass();
    while (clazz != null) {
      for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
        try {
          field.setAccessible(true);
          Object fieldValue = field.get(this);
          String fieldName = field.getName();
          processor.process(clazz, field, fieldName, fieldValue);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      clazz = clazz.getSuperclass();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("<");
    final Class[] c = {null};
    final boolean[] isFirstField = {true};
    iterateFields(new FieldProcessor() {
      @Override
      public void process(Class clazz, java.lang.reflect.Field field, String fieldName, Object fieldValue) {
        if (!clazz.equals(c[0])) {
          if (c[0] != null)
            sb.append("} ");
          c[0] = clazz;
          sb.append(clazz.getSimpleName()).append("{");
          isFirstField[0] = true;
        }
        if (fieldValue != null) {
          if (isFirstField[0])
            isFirstField[0] = false;
          else
            sb.append(", ");
          if (ObjectId.class.equals(field.getType())) {
            fieldValue = String.format("'%s'", fieldValue);
          } else if (field.isAnnotationPresent(EnumField.class)) {
            fieldValue = ((Enum) fieldValue).name();
          } else if (String.class.equals(field.getType()))
            fieldValue = String.format("'%s'", fieldValue);
          sb.append(fieldName).append(": ").append(fieldValue);
        }
      }
    });
    sb.append("}");
    sb.append(">");
    return sb.toString();
  }

  protected void updateFields(Iterable<Enum> fields) {
    UpdateBuilder builder = Managed.byId(this.getClass(), _id).exec.update();
    DBObject dbObj = ManagedService.toDBObject(this);
    for (Enum field : fields) {
      builder.set(field, dbObj.get(FieldName.get(field)));
    }
    builder.exec();
  }

  protected void updateFields(Enum... fields) {
    UpdateBuilder builder = Managed.byId(this.getClass(), _id).exec.update();
    DBObject dbObj = ManagedService.toDBObject(this);
    for (Enum field : fields) {
      builder.set(field, dbObj.get(FieldName.get(field)));
    }
    builder.exec();
  }

  public static enum Field {
    _ID,
    TOTAL
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null) return false;
    final boolean[] result = {true};
    iterateFields(new FieldProcessor() {
      @Override
      public void process(Class clazz, java.lang.reflect.Field field, String fieldName, Object fieldValue) {
        if (result[0]) {
          result[0] = clazz.isAssignableFrom(o.getClass());
          if (result[0]) {
            if (fieldValue == null) {
              try {
                result[0] = field.get(o) == null;
              } catch (IllegalAccessException e) {
                result[0] = false;
              }
            } else {
              try {
                result[0] = fieldValue.equals(field.get(o));
              } catch (IllegalAccessException e) {
                result[0] = false;
              }
            }
          }
        }
      }
    });
    return result[0];
  }

  @Override
  public int hashCode() {
    final int[] result = {super.hashCode()};
    iterateFields(new FieldProcessor() {
      @Override
      public void process(Class clazz, java.lang.reflect.Field field, String fieldName, Object fieldValue) {
        result[0] = 32 * result[0] + (fieldValue != null ? fieldValue.hashCode() : 0);
      }
    });
    return result[0];
  }

}
