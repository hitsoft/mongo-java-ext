package com.hitsoft.mongo.basic;

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
        _SS_GT,
        /**
         * Less Than (<)
         */
        _SS_LT,
        /**
         * Greater Than or Equal (>=)
         */
        _SS_GTE,
        /**
         * Less Than or Equal (<=)
         */
        _SS_LTE,
        /**
         * The $all operator is similar to $in, but instead of matching any value in the
         * specified array all values in the array must be matched.
         */
        _SS_ALL,
        /**
         * Check for existence (or lack thereof) of a field.
         */
        _SS_EXISTS,
        /**
         * The $mod operator allows you to do fast modulo queries to replace a common case for where clauses.
         */
        _SS_MOD,
        /**
         * Not Equals (!=)
         */
        _SS_NE,
        /**
         * The $in operator is analogous to the SQL IN modifier, allowing you to specify an array of possible matches.
         */
        _SS_IN,
        /**
         * The $nin operator is similar to $in except that it selects objects for which the specified field does
         * not have any value in the specified array.
         */
        _SS_NIN,
        /**
         * The $nor operator lets you use a boolean or expression to do queries.
         * You give $nor a list of expressions, none of which can satisfy the query.
         */
        _SS_NOR,
        /**
         * The $or operator lets you use boolean or in a query. You give $or an array of expressions,
         * any of which can satisfy the query.
         */
        _SS_OR,
        /**
         * The $and operator lets you use boolean and in a query. You give $and an array of expressions,
         * all of which must match to satisfy the query.
         */
        _SS_AND,
        /**
         * The $size operator matches any array with the specified number of elements.
         */
        _SS_SIZE,
        /**
         * The $type operator matches values based on their BSON type.
         */
        _SS_TYPE,
        /**
         * Regular Expression
         */
        _SS_REGEX,
        /**
         * Regular Expression Options
         */
        _SS_OPTIONS,
        /**
         * Not operator
         */
        _SS_NOT
    }

    private SearchBuilder singleAction(Enum field, Operator operator, Object value) {
        obj.add(field, DBObjectBuilder.single(operator, value));
        return this;
    }

    private SearchBuilder multipleConditionsAction(Enum field, Operator operator, Collection<SearchBuilder> conditions) {
        List<DBObjectExt> list = new ArrayList<DBObjectExt>();
        for (SearchBuilder condition : conditions) {
            list.add(condition.get());
        }
        return singleAction(field, operator, list);
    }

    public static SearchBuilder start() {
        return new SearchBuilder();
    }

    /**
     * Greater Than (>)
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder gt(Enum field, Object value) {
        return singleAction(field, Operator._SS_GT, value);
    }

    /**
     * Less Than (<)
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder lt(Enum field, Object value) {
        return singleAction(field, Operator._SS_LT, value);
    }

    /**
     * Greater Than or Equal (>=)
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder gte(Enum field, Object value) {
        return singleAction(field, Operator._SS_GTE, value);
    }

    /**
     * Less Than or Equal (<=)
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder lte(Enum field, Object value) {
        return singleAction(field, Operator._SS_LTE, value);
    }

    /**
     * Between excluding borders
     *
     * @param field condition field
     * @param from  value
     * @param to    value
     * @return self
     */
    public SearchBuilder between(Enum field, Object from, Object to) {
        obj.add(field, DBObjectBuilder.start()
                .add(Operator._SS_GT, from)
                .add(Operator._SS_LT, to)
                .get());
        return this;
    }

    /**
     * Extended between where user can specify wich border to include
     *
     * @param field       condition field
     * @param from        value
     * @param to          value
     * @param includeFrom if true then #from value will be included
     * @param includeTo   if true then #to value will be included
     * @return self
     */
    public SearchBuilder between(Enum field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        DBObjectBuilder cond = DBObjectBuilder.start();
        if (includeFrom)
            cond.add(Operator._SS_GTE, from);
        else
            cond.add(Operator._SS_GT, from);
        if (includeTo)
            cond.add(Operator._SS_LTE, to);
        else
            cond.add(Operator._SS_LT, to);
        obj.add(field, cond.get());
        return this;
    }

    /**
     * The $all operator is similar to $in, but instead of matching any value in the
     * specified array all values in the array must be matched.
     *
     * @param field     condition field
     * @param condition value
     * @return self
     */
    public SearchBuilder all(Enum field, SearchBuilder condition) {
        List<DBObjectExt> list = new ArrayList<DBObjectExt>();
        list.add(condition.get());
        return singleAction(field, Operator._SS_ALL, list);
    }

    /**
     * The $all operator is similar to $in, but instead of matching any value in the
     * specified array all values in the array must be matched.
     *
     * @param field      condition field
     * @param conditions value
     * @return self
     */
    public SearchBuilder all(Enum field, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(field, Operator._SS_ALL, conditions);
    }

    /**
     * Check for existence (or lack thereof) of a field.
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder exists(Enum field, boolean value) {
        return singleAction(field, Operator._SS_EXISTS, value);
    }

    /**
     * The $mod operator allows you to do fast modulo queries to replace a common case for where clauses.
     *
     * @param field condition field
     * @param mod   value
     * @param value value
     * @return self
     */
    public SearchBuilder mod(Enum field, int mod, int value) {
        List<Integer> param = new ArrayList<Integer>();
        param.add(mod);
        param.add(value);
        return singleAction(field, Operator._SS_MOD, value);
    }

    /**
     * Not Equals (!=)
     *
     * @param field condition field
     * @param value value
     * @return self
     */
    public SearchBuilder ne(Enum field, Object value) {
        return singleAction(field, Operator._SS_NE, value);
    }

    /**
     * The $in operator is analogous to the SQL IN modifier, allowing you to specify an array of possible matches.
     *
     * @param field  condition field
     * @param values value
     * @return self
     */
    public SearchBuilder in(Enum field, List values) {
        return singleAction(field, Operator._SS_IN, values);
    }

    /**
     * The $nin operator is similar to $in except that it selects objects for which the specified field does
     * not have any value in the specified array.
     *
     * @param field  condition field
     * @param values value
     * @return self
     */
    public SearchBuilder nin(Enum field, List values) {
        return singleAction(field, Operator._SS_NIN, values);
    }

    /**
     * The $nor operator lets you use a boolean or expression to do queries.
     * You give $nor a list of expressions, none of which can satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return self
     */
    public SearchBuilder nor(Enum field, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(field, Operator._SS_NOR, conditions);
    }

    /**
     * The $or operator lets you use boolean or in a query. You give $or an array of expressions,
     * any of which can satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return self
     */
    public SearchBuilder or(Enum field, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(field, Operator._SS_OR, conditions);
    }

    /**
     * The $and operator lets you use boolean and in a query. You give $and an array of expressions,
     * all of which must match to satisfy the query.
     *
     * @param field      condition field
     * @param conditions value
     * @return self
     */
    public SearchBuilder and(Enum field, Collection<SearchBuilder> conditions) {
        return multipleConditionsAction(field, Operator._SS_AND, conditions);
    }

    /**
     * The $size operator matches any array with the specified number of elements.
     *
     * @param field condition field
     * @param size  value
     * @return self
     */
    public SearchBuilder size(Enum field, int size) {
        return singleAction(field, Operator._SS_SIZE, size);
    }

    /**
     * The $type operator matches values based on their BSON type.
     *
     * @param field condition field
     * @param type  value
     * @return self
     */
    public SearchBuilder type(Enum field, FieldType type) {
        return singleAction(field, Operator._SS_TYPE, type.key());
    }

    /**
     * Regular Expression
     *
     * @param field condition field
     * @param expr  value
     * @return self
     */
    public SearchBuilder regex(Enum field, String expr) {
        Pattern pattern = Pattern.compile(expr);
        return singleAction(field, Operator._SS_REGEX, pattern);
    }

    public SearchBuilder regex(Enum field, String expr, int flags) {
        Pattern pattern = Pattern.compile(expr, flags);
        return singleAction(field, Operator._SS_REGEX, pattern);
    }

}
