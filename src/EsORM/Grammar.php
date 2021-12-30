<?php

namespace EsORM;

use Illuminate\Support\Arr;

class Grammar {

    /**
     * @var array|string[]
     */
    protected array $components = [
        'index'   => 'index',
        'scroll'  => 'scroll',
        '_source' => 'columns',
        'query'   => 'wheres',
        'aggs'    => 'aggs',
        'sort'    => 'orders',
        'from'    => 'offset',
        'size'    => 'limit',
    ];

    /**
     * es index
     *
     * @param Query $query
     * @return string
     */
    public function compileIndex (Query $query): string {
        return $query->index;
    }

    /**
     * 游标参数
     *
     * @param Query $query
     * @return string
     */
    public function compileScroll (Query $query): string {
        return $query->scroll;
    }

    /**
     * 查询指定字段
     *
     * @param Query $query
     * @return array
     */
    public function compileColumns (Query $query): array {
        return $query->columns;
    }

    /**
     * 条件查询
     *
     * @param Query $query
     * @return array
     */
    public function compileWheres (Query $query): array {
        $wherePriorityGroups = $this->wherePriorityGroup($query->wheres);

        $operation = count($wherePriorityGroups) === 1 ? 'must' : 'should';
        $bool = [];

        foreach ($wherePriorityGroups as $groups) {
            $mustArr = $mustNotArr = [];

            foreach ($groups as $group) {
                if ($group['type'] === 'Nested') {
                    $mustArr[] = $this->compileWheres($group['query']);
                } else {
                    if ($group['operator'] == 'neq') {
                        $mustNotArr[] = $this->whereLeaf($group['leaf'], $group['column'], $group['value'], $group['operator']);
                    } else {
                        $mustArr[] = $this->whereLeaf($group['leaf'], $group['column'], $group['value'], $group['operator']);
                    }
                }

                if (!empty($mustArr)) {
                    $bool['bool'][$operation][] = count($mustArr) === 1
                        ? array_shift($mustArr)
                        : ['bool' => ['must' => $mustArr]];
                }
                if (!empty($mustNotArr)) {
                    if ($operation == 'should') {
                        foreach ($mustNotArr as $not) {
                            $bool['bool'][$operation][] = [
                                'bool' => [
                                    'must_not' => $not,
                                ],
                            ];
                        }
                    } else {
                        $bool['bool']['must_not'] = $mustNotArr;
                    }
                }
            }
        }

        return $bool;
    }

    /**
     * where 条件优先组合。根据 or 查询来判断
     *
     * @param array $wheres
     * @return array
     */
    protected function wherePriorityGroup (array $wheres): array {
        $orIndex = array_keys(array_map(function ($where) {
            return $where['boolean'];
        }, $wheres), 'or');

        $lastIndex = $initIndex = 0;
        $groupArr = [];
        foreach ($orIndex as $index) {
            $groupArr[] = array_slice($wheres, $initIndex, $index - $initIndex);
            $lastIndex = $initIndex = $index;
        }

        $groupArr[] = array_slice($wheres, $lastIndex);

        return $groupArr;
    }

    /**
     * 不同查询数据组装
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @param string|null $operator
     * @return array|array[]
     */
    private function whereLeaf (string $leaf, string $column, $value, string $operator = null): array {
        if (strpos($column, '@') !== false) {
            $columnArr = explode('@', $column);
            $ret = [
                'nested' => [
                    'path' => $columnArr[0],
                ],
            ];
            $ret['nested']['query']['bool']['must'][] = $this->whereLeaf(
                $leaf, implode('.', $columnArr), $value, $operator
            );

            return $ret;
        }

        $whereLeafMethod = $this->getWhereMapByLeaf($leaf);
        if (empty($whereLeafMethod)) {
            return [];
        }

        return call_user_func([$this, $whereLeafMethod], $leaf, $column, $value, $operator);
    }

    /**
     * 获取对应匹配操作
     *
     * @param string $leaf
     * @return string
     */
    private function getWhereMapByLeaf (string $leaf): string {
        $map = [
            'term'         => 'whereLeafTerm',
            'match'        => 'whereLeafMatch',
            'terms'        => 'whereLeafTerms',
            'match_phrase' => 'whereLeafMatchPhrase',
            'range'        => 'whereLeafRange',
            'multi_match'  => 'whereLeafMultiMatch',
            'wildcard'     => 'whereLeafWildcard',
            'exists'       => 'whereLeafExists',
        ];

        return $map[$leaf] ?? '';
    }

    /**
     * 查询某个关键词的数据
     * 完全匹配，不进行分词器解析，文档中必须包含整个搜索的数据
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafTerm (string $leaf, string $column, $value): array {
        return $this->whereLeafBasicMatch($leaf, $column, $value);
    }

    /**
     * 查询匹配，会进行分词
     * @see https://www.elastic.co/guide/cn/elasticsearch/guide/current/match-query.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafMatch (string $leaf, string $column, $value): array {
        return $this->whereLeafBasicMatch($leaf, $column, $value);
    }

    /**
     * 查询某些关键词的数据。精确查找
     * @see https://www.elastic.co/guide/cn/elasticsearch/guide/current/_finding_multiple_exact_values.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafTerms (string $leaf, string $column, $value): array {
        return $this->whereLeafBasicMatch($leaf, $column, $value);
    }

    /**
     * 短语匹配查询，必须匹配短语中的所有分词
     * @see https://www.elastic.co/guide/cn/elasticsearch/guide/current/phrase-matching.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafMatchPhrase (string $leaf, string $column, $value): array {
        return $this->whereLeafBasicMatch($leaf, $column, $value);
    }

    /**
     * 匹配操作组合数据
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafBasicMatch (string $leaf, string $column, $value): array {
        return [
            $leaf => [
                $column => $value,
            ],
        ];
    }

    /**
     * 范围查询
     * @see https://www.elastic.co/guide/cn/elasticsearch/guide/current/_ranges.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @param string|null $operator
     * @return \array[][]
     */
    private function whereLeafRange (string $leaf, string $column, $value, string $operator = null): array {
        return [
            $leaf => [
                $column => is_array($value) ? $value : [$operator => $value],
            ],
        ];
    }

    /**
     * 多个字段执行相同查询
     * @see https://www.elastic.co/guide/cn/elasticsearch/guide/current/multi-match-query.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return array[]
     */
    private function whereLeafMultiMatch (string $leaf, string $column, $value): array {
        return [
            $leaf => [
                'query'  => $value,
                'fields' => (array)$column,
                'type'   => 'phrase',
            ],
        ];
    }

    /**
     * 模糊查询
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
     *
     * @param string $leaf
     * @param string $column
     * @param $value
     * @return \string[][]
     */
    private function whereLeafWildcard (string $leaf, string $column, $value): array {
        return [
            $leaf => [
                $column => '*'.$value.'*',
            ],
        ];
    }

    /**
     * 字段包含值
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
     *
     * @param string $leaf
     * @param string $column
     * @return \string[][]
     */
    private function whereLeafExists (string $leaf, string $column): array {
        return [
            $leaf => [
                'field' => $column,
            ],
        ];
    }

    /**
     * 聚合操作
     * @param Query $query
     * @return array
     * @example 'max_field' => ['max' => ['field' => 'value']]
     *
     */
    public function compileAggs (Query $query): array {
        $aggs = [];

        foreach ($query->aggs as $k => $v) {
            if (is_array($v)) {
                $aggs[$k] = $v;
            } else {
                $aggs[$v.'_'.$k] = [$v => ['field' => $k]];
            }
        }

        return $aggs;
    }

    /**
     * 排序。只能数字和日期可以排序
     * @param Query $query
     * @return array
     * @example 'field' => ['order' => 'asc']
     *
     */
    public function compileOrders (Query $query): array {
        $orders = [];

        foreach ($query->orders as $k => $v) {
            $orders[$k] = ['order' => $v];
        }

        return $orders;
    }

    /**
     * N页开始取
     *
     * @param Query $query
     * @return int
     */
    public function compileOffset (Query $query): int {
        return $query->offset;
    }

    /**
     * 取N条
     * @param Query $query
     * @return int
     */
    public function compileLimit (Query $query): int {
        return $query->limit;
    }

    /**
     * 执行各组件功能
     *
     * @param Query $query
     * @return array
     */
    public function compileComponents (Query $query): array {
        $body = [];

        foreach ($this->components as $k => $component) {
            if (!empty($query->$component)) {
                $method = 'compile'.ucfirst($component);

                $body[$k] = $this->$method($query);
            }
        }

        return $body;
    }

    /**
     * 创建数据组装
     *
     * @param Query $query
     * @param $id
     * @param array $data
     *
     * @return array
     */
    public function compileCreate (Query $query, array $data, $id): array {
        return array_merge([
            'id'   => $id,
            'body' => $data,
        ], $this->compileComponents($query));
    }

    /**
     * 编辑数据组装
     *
     * @param Query $query
     * @param $id
     * @param array $data
     *
     * @return array
     */
    public function compileUpdate (Query $query, $id, array $data): array {
        return array_merge([
            'id'   => $id,
            'body' => ['doc' => $data],
        ], $this->compileComponents($query));
    }

    /**
     * 删除数据组装
     *
     * @param Query $query
     * @param $id
     *
     * @return array
     */
    public function compileDelete (Query $query, $id): array {
        return array_merge([
            'id' => $id,
        ], $this->compileComponents($query));
    }

    /**
     * 查询单条数据组装
     *
     * @param Query $query
     * @param $id
     * @return array
     */
    public function compileGet (Query $query, $id): array {
        return array_merge([
            'id' => $id,
        ], $this->compileComponents($query));
    }

    /**
     * 查询多条数据组装
     *
     * @param Query $query
     *
     * @return array
     */
    public function compileSelect (Query $query): array {
        $body = $this->compileComponents($query);

        $index = Arr::pull($body, 'index');
        $scroll = Arr::pull($body, 'scroll');

        $params = [
            'index' => $index,
            'body'  => $body,
        ];
        if (!is_null($scroll)) {
            $params['scroll'] = $scroll;
        }

        return $params;
    }
}
