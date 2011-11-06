package com.hitsoft.mongo.basic;

import java.util.List;

/**
 * User: smeagol
 * Date: 04.11.11
 * Time: 13:52
 */
public class UpdateBuilder extends BaseBuilder {

    private static enum Operation {
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

    public static UpdateBuilder start() {
        return new UpdateBuilder();
    }

    private UpdateBuilder appendSingle(Operation operation, Enum field, Object value) {
        DBObjectExt res = obj.obj.getV(field).asObject();
        if (res == null)
            res = new BasicDBObjectExt();
        res.put(field, value);
        obj.add(operation, res);
        return this;
    }

    /**
     * increments field by the number value if field is present in the object, otherwise sets field to the number value.
     *
     * @param field updated field
     * @param value value
     * @return <code>this</code>
     */
    public UpdateBuilder inc(Enum field, Number value) {
        return appendSingle(Operation.$INC, field, value);
    }

    /**
     * sets field to value. All datatypes are supported with $set.
     *
     * @param field updated field
     * @param value value
     * @return <code>this</code>
     */
    public UpdateBuilder set(Enum field, Object value) {
        return appendSingle(Operation.$SET, field, value);
    }

    /**
     * Deletes a given field. v1.3+
     *
     * @param field updated field
     * @return <code>this</code>
     */
    public UpdateBuilder unset(Enum field) {
        return appendSingle(Operation.$UNSET, field, 1);
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
    public UpdateBuilder push(Enum field, Object value) {
        if (value instanceof List) {
            return appendSingle(Operation.$PUSH_ALL, field, value);
        } else {
            return appendSingle(Operation.$PUSH, field, value);
        }
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
    public UpdateBuilder addToSet(Enum field, Object value) {
        if (value instanceof List) {
            return appendSingle(Operation.$ADD_TO_SET, field, DBObjectBuilder.single(Operation.$EACH, value));
        } else {
            return appendSingle(Operation.$ADD_TO_SET, field, value);
        }
    }

    /**
     * removes the last element in an array
     *
     * @param field updated field
     * @return <code>this</code>
     */
    public UpdateBuilder popLast(Enum field) {
        return appendSingle(Operation.$POP, field, 1);
    }

    /**
     * removes the first element in an array
     *
     * @param field updated field
     * @return <code>this</code>
     */
    public UpdateBuilder popFirst(Enum field) {
        return appendSingle(Operation.$POP, field, -1);
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
    public UpdateBuilder pull(Enum field, Object value) {
        if (value instanceof List) {
            return appendSingle(Operation.$PULL_ALL, field, value);
        } else {
            return appendSingle(Operation.$PULL, field, value);
        }
    }

    /**
     * Renames the field with name 'old_field_name' to 'new_field_name'. Does not expand arrays to find a match for 'old_field_name'.
     *
     * @param oldField old field
     * @param newField new field
     * @return <code>this</code>
     */
    public UpdateBuilder rename(Enum oldField, Enum newField) {
        return appendSingle(Operation.$RENAME, oldField, FieldName.get(newField));
    }

}
