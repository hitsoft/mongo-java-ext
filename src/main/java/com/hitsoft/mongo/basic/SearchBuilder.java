package com.hitsoft.mongo.basic;

import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: smeagol
 * Date: 03.11.11
 * Time: 22:48
 */
public class SearchBuilder extends BaseBuilder {

    private static enum FieldType {
        DOUBLE(1),
        STRING(2),
        OBJECT(3),
        ARRAY(4),
        BINARY_DATA(5),
        OBJECT_ID(7),
        BOOLEAN(8),
        DATE(9),
        NULL(10),
        REGULAR_EXPRESSION(11),
        JAVASCRIPT_CODE(13),
        SYMBOL(14),
        JAVASCRIPT_CODE_WITH_SCOPE(15),
        INTEGER_32_BIT(16),
        TIMESTAMP(17),
        INTEGER_64_BIT(18),
        MIN_KEY(255),
        MAX_KEY(127);

        private int key;

        FieldType(int key) {
            this.key = key;
        }

        public int key() {
            return key;
        }
    }

    private static enum Operator {
        /**
         * Greater Than (>)
         */
        $GT,
        /**
         * Less Than (<)
         */
        $LT,
        /**
         * Greater Than or Equal (>=)
         */
        $GTE,
        /**
         * Less Than or Equal (<=)
         */
        $LTE,
        /**
         * The $all operator is similar to $in, but instead of matching any value in the
         * specified array all values in the array must be matched.
         */
        $ALL,
        /**
         * Check for existence (or lack thereof) of a field.
         */
        $EXISTS,
        /**
         * The $mod operator allows you to do fast modulo queries to replace a common case for where clauses.
         */
        $MOD,
        /**
         * Not Equals (!=)
         */
        $NE,
        /**
         * The $in operator is analogous to the SQL IN modifier, allowing you to specify an array of possible matches.
         */
        $IN,
        /**
         * The $nin operator is similar to $in except that it selects objects for which the specified field does
         * not have any value in the specified array.
         */
        $NIN,
        /**
         * The $nor operator lets you use a boolean or expression to do queries.
         * You give $nor a list of expressions, none of which can satisfy the query.
         */
        $NOR,
        /**
         * The $or operator lets you use boolean or in a query. You give $or an array of expressions,
         * any of which can satisfy the query.
         */
        $OR,
        /**
         * The $and operator lets you use boolean and in a query. You give $and an array of expressions,
         * all of which must match to satisfy the query.
         */
        $AND,
        /**
         * The $size operator matches any array with the specified number of elements.
         */
        $SIZE,
        /**
         * The $type operator matches values based on their BSON type.
         */
        $TYPE,
        /**
         * Regular Expression
         */
        $REGEX,
        /**
         * Regular Expression Options
         */
        $OPTIONS,
        /**
         * Not operator
         */
        $NOT
    }

    private SearchBuilder singleAction(Enum[] field, Operator operator, Object value) {
        DBObjectExt op = obj.obj.getV(field).asObject();
        if (op == null)
            op = DBObjectBuilder.single(operator, value);
        else
            op.put(operator, value);
        obj.add(field, op);
        return this;
    }

    private SearchBuilder singleAction(Enum field, Operator operator, Object value) {
        return singleAction(new Enum[]{field}, operator, value);
    }

    private SearchBuilder multipleConditionsAction(Enum[] field, Operator operator, Collection<SearchBuilder> conditions) {
        List<DBObjectExt> list = new ArrayList<DBObjectExt>();
        for (SearchBuilder condition : conditions) {
            list.add(condition.getExt());
        }
        return singleAction(field, operator, list);
    }
    private SearchBuilder multipleConditionsAction(Enum field, Operator operator, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(new Enum[]{field}, operator, conditions);
    }

    private SearchBuilder multipleConditionsAction(Operator operator, Collection<SearchBuilder> conditions) {
        List<DBObjectExt> list = new ArrayList<DBObjectExt>();
        for (SearchBuilder condition : conditions) {
            list.add(condition.getExt());
        }
        obj.add(operator, list.toArray());
        return this;
    }

    public static SearchBuilder start() {
        return new SearchBuilder();
    }

    public SearchBuilder equal(Enum[] field, Object value) {
        obj.add(field, value);
        return this;
    }

    public SearchBuilder equal(Enum field, Object value) {
        return equal(new Enum[]{field}, value);
    }

    public SearchBuilder equal(DBObjectExt sample) {
        obj.add(sample);
        return this;
    }

    /**
     * Greater Than (>)
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder greaterThan(Enum[] field, Object value) {
        return singleAction(field, Operator.$GT, value);
    }
    public SearchBuilder greaterThan(Enum field, Object value) {
        return greaterThan(new Enum[]{field}, value);
    }

    /**
     * Less Than (<)
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder lessThan(Enum[] field, Object value) {
        return singleAction(field, Operator.$LT, value);
    }
    public SearchBuilder lessThan(Enum field, Object value) {
        return lessThan(new Enum[]{field}, value);
    }

    /**
     * Greater Than or Equal (>=)
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder greaterOrEqualThan(Enum[] field, Object value) {
        return singleAction(field, Operator.$GTE, value);
    }
    public SearchBuilder greaterOrEqualThan(Enum field, Object value) {
        return greaterOrEqualThan(new Enum[]{field}, value);
    }

    /**
     * Less Than or Equal (<=)
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder lessOrEqualThan(Enum[] field, Object value) {
        return singleAction(field, Operator.$LTE, value);
    }
    public SearchBuilder lessOrEqualThan(Enum field, Object value) {
        return lessOrEqualThan(new Enum[]{field}, value);
    }

    /**
     * Between excluding borders
     *
     * @param field condition field
     * @param from  value
     * @param to    value
     * @return <code>this</code>
     */
    public SearchBuilder between(Enum[] field, Object from, Object to) {
        obj.add(field, DBObjectBuilder.start()
                .add(Operator.$GT, from)
                .add(Operator.$LT, to)
                .get());
        return this;
    }
    public SearchBuilder between(Enum field, Object from, Object to) {
        return between(new Enum[]{field}, from, to);
    }

    /**
     * Extended between where user can specify wich border to include
     *
     * @param field       condition field
     * @param from        value
     * @param to          value
     * @param includeFrom if true then #from value will be included
     * @param includeTo   if true then #to value will be included
     * @return <code>this</code>
     */
    public SearchBuilder between(Enum[] field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        DBObjectBuilder cond = DBObjectBuilder.start();
        if (includeFrom)
            cond.add(Operator.$GTE, from);
        else
            cond.add(Operator.$GT, from);
        if (includeTo)
            cond.add(Operator.$LTE, to);
        else
            cond.add(Operator.$LT, to);
        obj.add(field, cond.get());
        return this;
    }
    public SearchBuilder between(Enum field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        return between(new Enum[]{field}, from, to, includeFrom, includeTo);
    }

    /**
     * The $all operator is similar to $in, but instead of matching any value in the
     * specified array all values in the array must be matched.
     *
     * @param field     condition field
     * @param condition value
     * @return <code>this</code>
     */
    public SearchBuilder all(Enum[] field, SearchBuilder condition) {
        List<DBObjectExt> list = new ArrayList<DBObjectExt>();
        list.add(condition.getExt());
        return singleAction(field, Operator.$ALL, list);
    }
    public SearchBuilder all(Enum field, SearchBuilder condition) {
        return all(new Enum[]{field}, condition);
    }

    /**
     * The $all operator is similar to $in, but instead of matching any value in the
     * specified array all values in the array must be matched.
     *
     * @param field      condition field
     * @param conditions value
     * @return <code>this</code>
     */
    public SearchBuilder all(Enum[] field, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(field, Operator.$ALL, conditions);
    }
    public SearchBuilder all(Enum field, Collection<SearchBuilder> conditions) {
        return all(new Enum[]{field}, conditions);
    }

    /**
     * Check for existence (or lack thereof) of a field.
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder exists(Enum[] field, boolean value) {
        return singleAction(field, Operator.$EXISTS, value);
    }
    public SearchBuilder exists(Enum field, boolean value) {
        return exists(new Enum[]{field}, value);
    }

    /**
     * The $mod operator allows you to do fast modulo queries to replace a common case for where clauses.
     *
     * @param field condition field
     * @param mod   value
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder mod(Enum[] field, int mod, int value) {
        List<Integer> param = new ArrayList<Integer>();
        param.add(mod);
        param.add(value);
        return singleAction(field, Operator.$MOD, param);
    }
    public SearchBuilder mod(Enum field, int mod, int value) {
        return mod(new Enum[]{field}, mod, value);
    }

    /**
     * Not Equals (!=)
     *
     * @param field condition field
     * @param value value
     * @return <code>this</code>
     */
    public SearchBuilder notEquals(Enum[] field, Object value) {
        return singleAction(field, Operator.$NE, value);
    }
    public SearchBuilder notEquals(Enum field, Object value) {
        return notEquals(new Enum[]{field}, value);
    }

    /**
     * The $in operator is analogous to the SQL IN modifier, allowing you to specify an array of possible matches.
     *
     * @param field  condition field
     * @param values value
     * @return <code>this</code>
     */
    public SearchBuilder in(Enum[] field, Collection values) {
        return singleAction(field, Operator.$IN, values);
    }
    public SearchBuilder in(Enum field, Collection values) {
        return in(new Enum[]{field}, values);
    }

    /**
     * The $nin operator is similar to $in except that it selects objects for which the specified field does
     * not have any value in the specified array.
     *
     * @param field  condition field
     * @param values value
     * @return <code>this</code>
     */
    public SearchBuilder notIn(Enum[] field, Collection values) {
        return singleAction(field, Operator.$NIN, values);
    }
    public SearchBuilder notIn(Enum field, Collection values) {
        return notIn(new Enum[]{field}, values);
    }

    /**
     * The $nor operator lets you use a boolean or expression to do queries.
     * You give $nor a list of expressions, none of which can satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return <code>this</code>
     */
    public SearchBuilder nor(Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(Operator.$NOR, conditions);
    }

    /**
     * The $or operator lets you use boolean or in a query. You give $or an array of expressions,
     * any of which can satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return <code>this</code>
     */
    public SearchBuilder or(Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(Operator.$OR, conditions);
    }

    /**
     * The $and operator lets you use boolean and in a query. You give $and an array of expressions,
     * all of which must match to satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return <code>this</code>
     */
    public SearchBuilder and(Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(Operator.$AND, conditions);
    }

    /**
     * The $size operator matches any array with the specified number of elements.
     *
     * @param field condition field
     * @param size  value
     * @return <code>this</code>
     */
    public SearchBuilder size(Enum[] field, int size) {
        return singleAction(field, Operator.$SIZE, size);
    }
    public SearchBuilder size(Enum field, int size) {
        return size(new Enum[]{field}, size);
    }

    /**
     * The $type operator matches values based on their BSON type.
     *
     * @param field condition field
     * @param type  value
     * @return <code>this</code>
     */
    public SearchBuilder type(Enum[] field, FieldType type) {
        return singleAction(field, Operator.$TYPE, type.key());
    }
    public SearchBuilder type(Enum field, FieldType type) {
        return type(new Enum[]{field}, type);
    }

    /**
     * Regular Expression
     *
     * @param field condition field
     * @param expr  value
     * @return <code>this</code>
     */
    public SearchBuilder regex(Enum[] field, String expr) {
        Pattern pattern = Pattern.compile(expr);
        return singleAction(field, Operator.$REGEX, pattern);
    }
    public SearchBuilder regex(Enum field, String expr) {
        return regex(new Enum[]{field}, expr);
    }

    public SearchBuilder regex(Enum[] field, String expr, int flags) {
        Pattern pattern = Pattern.compile(expr, flags);
        return singleAction(field, Operator.$REGEX, pattern);
    }
    public SearchBuilder regex(Enum field, String expr, int flags) {
        return regex(new Enum[]{field}, expr, flags);
    }

    public static SearchBuilder fromDBObject(DBObject obj) {
        SearchBuilder result = SearchBuilder.start();
        result.obj.obj.asDBObject().putAll(obj.toMap());
        return result;
    }
}
