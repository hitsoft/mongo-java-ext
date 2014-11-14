package com.hitsoft.mongo.basic;

import com.hitsoft.mongo.repository.Basic;
import com.mongodb.DBObject;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 28.01.11
 * Time: 16:33
 */
public class UpdateBuilder extends BaseBuilder {

  private final Basic repository;
  private final SearchBuilder searchBuilder;

  public UpdateBuilder(Basic repository, SearchBuilder searchBuilder) {
    this.repository = repository;
    this.searchBuilder = searchBuilder;
  }

  public static UpdateBuilder start() {
    return new UpdateBuilder(null, null);
  }

  private enum Operation {
    $INC,
    $SET,
    $UNSET,
    $PUSH,
    $PUSH_ALL,
    $ADD_TO_SET,
    $EACH,
    $POP,
    $PULL,
    $PULL_ALL,
    $RENAME,
    $BIT
  }

  private UpdateBuilder appendSingle(Operation operation, Enum[] field, Object value) {
    DBObjectExt res = obj.obj.getV(operation).asObject();
    if (res == null)
      res = new BasicDBObjectExt();
    res.put(field, value);
    obj.add(operation, res);
    return this;
  }
  private UpdateBuilder appendSingle(Operation operation, Enum field, Object value) {
    return appendSingle(operation, new Enum[]{field}, value);
  }

  /**
   * increments field by the number value if field is present in the object, otherwise sets field to the number value.
   *
   * @param field updated field
   * @param value value
   * @return <code>this</code>
   */
  public UpdateBuilder inc(Enum[] field, Number value) {
    return appendSingle(Operation.$INC, field, value);
  }
  public UpdateBuilder inc(Enum field, Number value) {
    return inc(new Enum[]{field}, value);
  }

  /**
   * sets field to value. All datatypes are supported with $set.
   *
   * @param field updated field
   * @param value value
   * @return <code>this</code>
   */
  public UpdateBuilder set(Enum[] field, Object value) {
    return appendSingle(Operation.$SET, field, value);
  }
  public UpdateBuilder set(Enum field, Object value) {
    return set(new Enum[]{field}, value);
  }

  /**
   * Устанавливает сразу несколько полей
   *
   * @param object
   * @return <code>this</code>
   */
  public UpdateBuilder set(DBObject object) {
    obj.add(Operation.$SET, object);
    return this;
  }

  /**
   * Deletes a given field. v1.3+
   *
   * @param field updated field
   * @return <code>this</code>
   */
  public UpdateBuilder unset(Enum[] field) {
    return appendSingle(Operation.$UNSET, field, 1);
  }
  public UpdateBuilder unset(Enum field) {
    return unset(new Enum[]{field});
  }

  /**
   * appends value to field, if field is an existing array, otherwise sets field to the array [value]
   * if field is not present. If field is present but is not an array, an error condition is raised.
   * <p/>
   * if value is instance of List then appends each value in value_array to field, if field is an
   * existing array, otherwise sets field to the array value_array if field is not present. If field
   * is present but is not an array, an error condition is raised.
   *
   * @param field updated field
   * @param value value
   * @return <code>this</code>
   */
  public UpdateBuilder push(Enum[] field, Object value) {
    if (value instanceof List) {
      return appendSingle(Operation.$PUSH_ALL, field, value);
    } else {
      return appendSingle(Operation.$PUSH, field, value);
    }
  }
  public UpdateBuilder push(Enum field, Object value) {
    return push(new Enum[]{field}, value);
  }

  /**
   * Adds value to the array only if its not in the array already, if field is an existing array,
   * otherwise sets field to the array value if field is not present. If field is present but is not an
   * array, an error condition is raised.
   * <p/>
   * To add many values value should be instance of List
   *
   * @param field updated field
   * @param value value
   * @return <code>this</code>
   */
  public UpdateBuilder addToSet(Enum[] field, Object value) {
    if (value instanceof List) {
      return appendSingle(Operation.$ADD_TO_SET, field, com.hitsoft.mongo.basic.DBObjectBuilder.single(Operation.$EACH, value));
    } else {
      return appendSingle(Operation.$ADD_TO_SET, field, value);
    }
  }
  public UpdateBuilder addToSet(Enum field, Object value) {
    return addToSet(new Enum[]{field}, value);
  }

  /**
   * removes the last element in an array
   *
   * @param field updated field
   * @return <code>this</code>
   */
  public UpdateBuilder popLast(Enum[] field) {
    return appendSingle(Operation.$POP, field, 1);
  }
  public UpdateBuilder popLast(Enum field) {
    return popLast(new Enum[]{field});
  }

  /**
   * removes the first element in an array
   *
   * @param field updated field
   * @return <code>this</code>
   */
  public UpdateBuilder popFirst(Enum[] field) {
    return appendSingle(Operation.$POP, field, -1);
  }
  public UpdateBuilder popFirst(Enum field) {
    return popFirst(new Enum[]{field});
  }

  /**
   * removes all occurrences of value from field, if field is an array. If field is
   * present but is not an array, an error condition is raised.
   * <p/>
   * if value is instance of List then removes all occurrences of each value in
   * value_array from field, if field is an array.
   * <p/>
   * value can also be instance of SearchBuilder in this case the matched values will be
   * removed from the array
   *
   * @param field updated field
   * @param value value
   * @return <code>this</code>
   */
  public UpdateBuilder pull(Enum[] field, Object value) {
    if (value instanceof List) {
      return appendSingle(Operation.$PULL_ALL, field, value);
    } else {
      return appendSingle(Operation.$PULL, field, value);
    }
  }
  public UpdateBuilder pull(Enum field, Object value) {
    return pull(new Enum[]{field}, value);
  }

  /**
   * Renames the field with name 'old_field_name' to 'new_field_name'. Does not expand arrays to find a match for 'old_field_name'.
   *
   * @param oldField old field
   * @param newField new field
   * @return <code>this</code>
   */
  public UpdateBuilder rename(Enum[] oldField, Enum[] newField) {
    return appendSingle(Operation.$RENAME, oldField, FieldName.get(newField));
  }
  public UpdateBuilder rename(Enum oldField, Enum newField) {
    return rename(new Enum[]{oldField}, new Enum[]{newField});
  }
  public UpdateBuilder rename(Enum oldField, Enum[] newField) {
    return rename(new Enum[]{oldField}, newField);
  }
  public UpdateBuilder rename(Enum[] oldField, Enum newField) {
    return rename(oldField, new Enum[]{newField});
  }

  public void exec() {
    assert (repository != null) && (searchBuilder != null) : "You can not use exec() in standalone mode.";
    repository.collection.update(searchBuilder.get(), this.get(), false, true);
  }
}
