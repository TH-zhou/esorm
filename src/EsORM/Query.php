<?php

namespace EsORM;

use Closure;

class Query {

    /**
     * @var string
     */
    public string $index;

    /**
     * @var string
     */
    public string $scroll;

    /**
     * @var array
     */
    public array $wheres = [];

    /**
     * @var array
     */
    public array $columns = [];

    /**
     * @var array
     */
    public array $orders = [];

    /**
     * @var int
     */
    public int $offset;

    /**
     * @var int
     */
    public int $limit;

    /**
     * @var array
     */
    public array $aggs = [];

    /**
     * @var array|string[]
     */
    public array $operators = [
        '='  => 'eq',
        '!=' => 'neq',
    ];

    /**
     * @var array|string[]
     */
    public array $operatorsRange = [
        '>'  => 'gt',
        '>=' => 'gte',
        '<'  => 'lt',
        '<=' => 'lte',
    ];

    /**
     * @var Grammar
     */
    protected Grammar $grammar;

    public function __construct (Grammar $grammar) {
        $this->grammar = $grammar;

        $this->operators = array_merge($this->operators, $this->operatorsRange);
    }

    /**
     * @return Query
     */
    public function newQuery (): self {
        return new static($this->grammar);
    }

    /**
     * @return Grammar
     */
    public function getGrammar (): Grammar {
        return $this->grammar;
    }

    /**
     * 设置es index
     *
     * @param string $index
     * @return $this
     */
    public function index (string $index): Query {
        $this->index = $index;

        return $this;
    }

    /**
     * 设置游标
     *
     * @param string $scroll
     * @return $this
     */
    public function scroll (string $scroll): Query {
        $this->scroll = $scroll;

        return $this;
    }

    /**
     * 分词匹配查询
     *
     * @param $field
     * @param $value
     * @param string $boolean
     *
     * @return Query
     */
    public function whereMatch ($field, $value, string $boolean = 'and'): Query {
        return $this->where($field, '=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     *
     * @return Query
     */
    public function orWhereMatch ($field, $value): Query {
        return $this->whereMatch($field, $value, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotMatch ($field, $value, string $boolean = 'and'): Query {
        return $this->where($field, '!=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereNotMatch ($field, $value): Query {
        return $this->whereNotMatch($field, $value, 'or');
    }

    /**
     * 不分词匹配查询
     *
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereMatchKeyword ($field, $value, string $boolean = 'and'): Query {
        return $this->where(sprintf("%s.%s", $field, 'keyword'), '=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereMatchKeyword ($field, $value): Query {
        return $this->whereMatchKeyword($field, $value, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotMatchKeyword ($field, $value, string $boolean = 'and'): Query {
        return $this->where(sprintf("%s.%s", $field, 'keyword'), '!=', $value, 'match', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereNotMatchKeyword ($field, $value): Query {
        return $this->whereNotMatchKeyword($field, $value, 'or');
    }

    /**
     * 精确匹配查询
     *
     * @param $field
     * @param $value
     * @param string $boolean
     *
     * @return Query
     */
    public function whereTerm ($field, $value, string $boolean = 'and'): Query {
        return $this->where($field, '=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereTerm ($field, $value): Query {
        return $this->whereTerm($field, $value, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotTerm ($field, $value, string $boolean = 'and'): Query {
        return $this->where($field, '!=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereNotTerm ($field, $value): Query {
        return $this->whereNotTerm($field, $value, 'or');
    }

    /**
     * 不分词精确匹配查询
     *
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereTermKeyword ($field, $value, string $boolean = 'and'): Query {
        return $this->where(sprintf("%s.%s", $field, 'keyword'), '=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereTermKeyword ($field, $value): Query {
        return $this->whereTermKeyword($field, $value, 'or');
    }

    /**
     * @param $field
     * @param $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotTermKeyword ($field, $value, string $boolean = 'and'): Query {
        return $this->where(sprintf("%s.%s", $field, 'keyword'), '!=', $value, 'term', $boolean);
    }

    /**
     * @param $field
     * @param $value
     * @return $this
     */
    public function orWhereNotTermKeyword ($field, $value): Query {
        return $this->whereNotTermKeyword($field, $value, 'or');
    }

    /**
     * 多个值精确匹配查询
     *
     * @param $field
     * @param array $value
     * @param string $boolean
     * @return $this
     */
    public function whereTerms ($field, array $value, string $boolean = 'and'): Query {
        return $this->where($field, null, $value, 'terms', $boolean);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereTerms ($field, array $value): Query {
        return $this->whereTerms($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotTerms ($field, array $value, string $boolean = 'and'): Query {
        return $this->where($field, '!=', $value, 'terms', $boolean);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotTerms ($field, array $value): Query {
        return $this->whereNotTerms($field, $value);
    }

    /**
     * @param $field
     * @param array $value
     * @param bool $boolean
     *
     * @return Query
     */
    public function whereIn ($field, array $value, $boolean = 'and'): Query {
        return $this->whereTerms($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param array $value
     *
     * @return Query
     */
    public function orWhereIn ($field, array $value): Query {
        return $this->whereIn($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @return $this
     */
    public function whereNotIn ($field, array $value, string $boolean = 'and'): Query {
        return $this->whereNotTerms($field, $value, $boolean);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotIn ($field, array $value): Query {
        return $this->whereNotIn($field, $value, 'or');
    }

    /**
     * 范围查询
     *
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     *
     * @return Query
     */
    public function whereRange ($field, $operator = null, $value = null, string $boolean = 'and'): Query {
        return $this->where($field, $operator, $value, 'range', $boolean);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     *
     * @return Query
     */
    public function orWhereRange ($field, $operator = null, $value = null): Query {
        return $this->whereRange($field, $operator, $value, 'or');
    }

    /**
     * 区间查询
     *
     * @param $field
     * @param array $values
     * @param string $boolean
     *
     * @return Query
     */
    public function whereBetween ($field, array $values, string $boolean = 'and'): Query {
        return $this->where($field, null, $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     *
     * @return Query
     */
    public function orWhereBetween ($field, array $values): Query {
        return $this->whereBetween($field, $values, 'or');
    }

    /**
     * @param $field
     * @param array $values
     * @param string $boolean
     *
     * @return Query
     */
    public function whereNotBetween ($field, array $values, string $boolean = 'and'): Query {
        return $this->where($field, '!=', $values, 'range', $boolean);
    }

    /**
     * @param $field
     * @param array $values
     *
     * @return Query
     */
    public function orWhereNotBetween ($field, array $values): Query {
        return $this->whereNotBetween($field, $values, 'or');
    }

    /**
     * 验证字段是否存在
     *
     * @param $field
     * @param string $boolean
     *
     * @return Query
     */
    public function whereExists ($field, string $boolean = 'and'): Query {
        return $this->where($field, '=', '', 'exists', $boolean);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereExists ($field): Query {
        return $this->whereExists($field, 'or');
    }

    /**
     * @param $field
     * @param string $boolean
     * @return $this
     */
    public function whereNotExists ($field, string $boolean = 'and'): Query {
        return $this->where($field, '!=', '', 'exists', $boolean);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereNotExists ($field): Query {
        return $this->whereNotExists($field, 'or');
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $leaf
     *
     * @return Query
     */
    public function orWhere ($field, $operator = null, $value = null, string $leaf = 'term'): Query {
        if (func_num_args() === 2) {
            [$value, $operator] = [$operator, '='];
        }

        return $this->where($field, $operator, $value, $leaf, 'or');
    }

    /**
     * 设置条件
     *
     * @note 当column字段类型是为text，不想分词处理的时候，手动 .keyword，eg：name.keyword
     * @note 当value是大写的时候，es创建倒排索引的时候，就已经将大写转为小写了，而后写入索引
     *
     * @param $column
     * @param null $operator
     * @param null $value
     * @param string $leaf
     * @param string $boolean
     * @return $this
     */
    public function where ($column, $operator = null, $value = null, string $leaf = 'term', string $boolean = 'and'): Query {
        if ($column instanceof Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (is_array($column)) {
            return $this->addArrayOfWheres($column);
        }

        if (func_num_args() === 2) {
            [$value, $operator] = [$operator, '='];
        }

        if (is_array($operator)) {
            [$value, $operator] = [$operator, null];
        }

        if (in_array($operator, array_keys($this->operatorsRange))) {
            $leaf = 'range';
        }

        $operator = $operator ? $this->operators[$operator] : $operator;

        if (is_array($value) && $leaf === 'range') {
            if ($value[1] < $value[0]) {
                [$value[1], $value[0]] = $value;
            }

            $value = [
                $this->operators['>='] => $value[0],
                $this->operators['<='] => $value[1],
            ];
        }

        $type = 'Basic';

        $this->wheres[] = compact(
            'type',
            'column',
            'leaf',
            'value',
            'boolean',
            'operator'
        );

        return $this;
    }

    /**
     * @param Closure $callback
     * @param string $boolean
     *
     * @return Query
     */
    public function whereNested (Closure $callback, string $boolean): Query {
        $query = $this->newQuery();

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * 将一组 where 子句添加到查询中
     *
     * @param $column
     * @param string $boolean
     *
     * @return Query
     */
    protected function addArrayOfWheres ($column, string $boolean = 'and'): Query {
        foreach ($column as $key => $value) {
            if (is_numeric($key) && is_array($value)) {
                $this->where(...array_values($value));
            } else {
                $leaf = is_array($value) ? 'terms' : 'term';
                $this->where($key, '=', $value, $leaf, $boolean);
            }
        }

        return $this;
    }

    /**
     * @param Query $query
     * @param string $boolean
     *
     * @return Query
     */
    protected function addNestedWhereQuery (Query $query, string $boolean = 'and'): Query {
        if (count($query->wheres)) {
            $type = 'Nested';
            $this->wheres[] = compact('type', 'query', 'boolean');
        }

        return $this;
    }

    /**
     * 设置需要查询的字段
     *
     * @param string|array $columns
     *
     * @return Query
     */
    public function select ($columns): Query {
        $this->columns = is_array($columns) ? $columns : func_get_args();

        return $this;
    }

    /**
     * 排序
     *
     * @param string $field
     * @param $sort
     *
     * @return Query
     */
    public function orderBy (string $field, $sort): self {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * 设置聚合
     *
     * @param string|array $field
     * @param $type
     *
     * @return Query
     */
    public function aggBy ($field, $type = null): Query {
        if (is_array($field)) {
            return $this->addArrayOfAggs($field);
        }

        $this->aggs[$field] = $type;

        return $this;
    }

    /**
     * @param array $field
     * @return $this
     */
    protected function addArrayOfAggs (array $field): Query {
        foreach ($field as $key => $value) {
            if (is_numeric($key) && is_array($value)) {
                $this->aggBy($value);
            } else {
                $this->aggs[$key] = $value;
            }
        }

        return $this;
    }

    /**
     * 设置从N页开始取
     *
     * @param int $value
     *
     * @return Query
     */
    public function offset (int $value): self {
        $this->offset = max(0, $value);

        return $this;
    }

    /**
     * 设置取N条
     *
     * @param int $value
     *
     * @return Query
     */
    public function limit (int $value): self {
        if ($value >= 0) {
            $this->limit = $value;
        }

        return $this;
    }
}
