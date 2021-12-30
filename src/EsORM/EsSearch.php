<?php

namespace EsORM;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Illuminate\Support\Collection;
use Exception;

/**
 * @method EsSearch index (string $index)
 * @method EsSearch scroll (string $scroll)
 * @method EsSearch whereMatch ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereMatch ($field, $value)
 * @method EsSearch whereNotMatch ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereNotMatch ($field, $value)
 * @method EsSearch whereMatchKeyword ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereMatchKeyword ($field, $value)
 * @method EsSearch whereNotMatchKeyword ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereNotMatchKeyword ($field, $value)
 * @method EsSearch whereTerm ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereTerm ($field, $value)
 * @method EsSearch whereNotTerm ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereNotTerm ($field, $value)
 * @method EsSearch whereTermKeyword ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereTermKeyword ($field, $value)
 * @method EsSearch whereNotTermKeyword ($field, $value, string $boolean = 'and')
 * @method EsSearch orWhereNotTermKeyword ($field, $value)
 * @method EsSearch whereTerms ($field, array $value, string $boolean = 'and')
 * @method EsSearch orWhereTerms ($field, array $value)
 * @method EsSearch whereNotTerms ($field, array $value, string $boolean = 'and')
 * @method EsSearch orWhereNotTerms ($field, array $value)
 * @method EsSearch whereIn ($field, array $value)
 * @method EsSearch orWhereIn ($field, array $value)
 * @method EsSearch whereNotIn ($field, array $value, string $boolean = 'and')
 * @method EsSearch orWhereNotIn ($field, array $value)
 * @method EsSearch whereRange ($field, $operator = null, $value = null, string $boolean = 'and')
 * @method EsSearch orWhereRange ($field, $operator = null, $value = null)
 * @method EsSearch whereBetween ($field, array $values, string $boolean = 'and')
 * @method EsSearch orWhereBetween ($field, array $values)
 * @method EsSearch whereNotBetween ($field, array $values, string $boolean = 'and')
 * @method EsSearch orWhereNotBetween ($field, array $values)
 * @method EsSearch whereExists ($field, string $boolean = 'and')
 * @method EsSearch orWhereExists ($field)
 * @method EsSearch whereNotExists ($field, string $boolean = 'and')
 * @method EsSearch orWhereNotExists ($field)
 * @method EsSearch orWhere ($field, $operator = null, $value = null, string $leaf = 'term')
 * @method EsSearch where ($column, $operator = null, $value = null, string $leaf = 'term', string $boolean = 'and')
 * @method EsSearch select ($columns)
 * @method EsSearch orderBy (string $field, $sort)
 * @method EsSearch aggBy ($field, $type = null)
 * @method EsSearch offset (int $value)
 * @method EsSearch limit (int $value)
 */
class EsSearch {

    private static array $instance = [];

    public array $config;

    private Client $client;

    private Query $query;

    /**
     * @param array $config
     */
    private function __construct (array $config = []) {
        $this->config = array_merge([
            'hosts'   => ['127.0.0.1:9200'],
            'retries' => 2,
        ], $config);

        $this->client = $this->newElasticSearchClient();

        $this->query = $this->newQuery();
    }

    private function __clone () { }

    /**
     * @param array $config
     * @return EsSearch
     */
    public static function getInstance (array $config = []): EsSearch {
        $key = self::configCRCKey($config);
        if (!isset(self::$instance[$key])) {
            self::$instance[$key] = new self($config);
        }

        return self::$instance[$key];
    }

    /**
     * 配置项key
     *
     * @param array $config
     * @return string
     */
    private static function configCRCKey (array $config): string {
        asort($config);

        return (string)crc32(json_encode($config));
    }

    /**
     * @return Client
     */
    private function newElasticSearchClient (): Client {
        return ClientBuilder::fromConfig($this->config);
    }

    /**
     * @return Query
     */
    private function newQuery (): Query {
        return new Query(new Grammar());
    }

    /**
     * 更换配置
     *
     * @param array $config
     * @return EsSearch
     */
    public function config (array $config): EsSearch {
        $cls = new self($config);

        self::$instance[$this->configCRCKey($config)] = $cls;

        return $cls;
    }

    /**
     * @return Client
     */
    public function getClient (): Client {
        return $this->client;
    }

    /**
     * 创建
     *
     * @param array $data
     * @param null $id
     * @return array
     * @throws Exception
     */
    public function create (array $data, $id = null): array {
        $id = $id ?? rand(1000, 100000);

        $result = $this->runQuery(
            $this->query->getGrammar()->compileCreate($this->query, $data, $id), 'create'
        );

        if (!isset($result['result']) || $result['result'] !== 'created') {
            throw new Exception('Create error');
        }

        return array_merge($data, [
            '_id'     => $id,
            '_result' => $result,
        ]);
    }

    /**
     * 编辑
     *
     * @param $id
     * @param array $data
     * @return bool
     * @throws Exception
     */
    public function update ($id, array $data): bool {
        $result = $this->runQuery(
            $this->query->getGrammar()->compileUpdate($this->query, $id, $data), 'update'
        );

        return isset($result['result']) && $result['result'] == 'updated';
    }

    /**
     * 删除
     *
     * @param $id
     * @return bool
     * @throws Exception
     */
    public function delete ($id): bool {
        $result = $this->runQuery($this->query->getGrammar()->compileDelete($this->query, $id), 'delete');

        return isset($result['result']) && $result['result'] == 'deleted';
    }

    /**
     * 获取总数
     *
     * @return int
     * @throws Exception
     */
    public function count (): int {
        $result = $this->runQuery($this->query->getGrammar()->compileSelect($this->query), 'count');

        return $result['count'];
    }

    /**
     * 根据id查询单条数据
     *
     * @param $id
     * @param array $columns
     * @return Collection
     * @throws Exception
     */
    public function find ($id, array $columns = []): Collection {
        $this->query->select(array_merge($this->query->columns, $columns));

        $result = $this->runQuery($this->query->getGrammar()->compileGet($this->query, $id), 'get');

        return collect($this->getOriginData($result));
    }

    /**
     * 获取单条数据
     *
     * @param array $columns
     * @return Collection
     * @throws Exception
     */
    public function first (array $columns = []): Collection {
        $this->query->limit(1);

        return collect($this->get($columns)->first());
    }

    /**
     * 获取多条数据
     *
     * @param array $columns
     * @param bool $isReturnTotal true返回总数，默认false
     * @return Collection
     * @throws Exception
     */
    public function get (array $columns = [], bool $isReturnTotal = false): Collection {
        $this->query->select($columns);

        $results = $this->search();
        $returnData = $this->getSearchResultData($results);

        if ($isReturnTotal) {
            $returnData['total'] = $results['hits']['total']['value'] ?? 0;
        }

        return collect($returnData);
    }

    /**
     * 将给定的查询分页到一个简单的分页器中
     *
     * @param int $page
     * @param int $pageSize
     * @return Collection
     * @throws Exception
     */
    public function paginate (int $page, int $pageSize = 15): Collection {
        $from = ($page * $pageSize) - $pageSize;

        if (empty($this->query->offset)) {
            $this->query->offset($from);
        }

        if (empty($this->query->limit)) {
            $this->query->limit($pageSize);
        }

        $results = $this->search();

        $data = $this->getMetaData($results);

        $maxPage = intval(ceil($results['hits']['total']['value'] / $pageSize));

        return collect([
            'total'        => $results['hits']['total']['value'],
            'page_size'    => $pageSize,
            'current_page' => $page,
            'next_page'    => $page < $maxPage ? $page + 1 : $maxPage,
            'total_pages'  => $maxPage,
            'from'         => $from,
            'to'           => $from + $pageSize,
            'hits'         => $data,
        ]);
    }

    /**
     * 分块查询结果
     *
     * @param callable $callback
     * @param int $limit
     * @param string $scroll
     * @return bool
     * @throws Exception
     */
    public function chunk (callable $callback, $limit = 2000, $scroll = '10m'): bool {
        if (empty($this->query->scroll)) {
            $this->query->scroll($scroll);
        } else {
            $scroll = $this->query->scroll;
        }

        if (empty($this->query->limit)) {
            $this->query->limit($limit);
        } else {
            $limit = $this->query->limit;
        }

        $results = $this->search();

        if ($results['hits']['total']['value'] === 0) {
            return false;
        }

        // 已经查询过一次，当前总数就是当前数量
        $total = $limit;

        $whileNum = intval(floor($results['hits']['total']['value'] / $total));

        do {
            if (call_user_func($callback, $this->getMetaData($results)) === false) {
                return false;
            }

            $results = $this->runQuery(['scroll_id' => $results['_scroll_id'], 'scroll' => $scroll], 'scroll');
            $total += count($results['hits']['hits']);
        } while ($whileNum--);

        return true;
    }

    /**
     * @return array
     * @throws Exception
     */
    public function search (): array {
        return $this->runQuery($this->query->getGrammar()->compileSelect($this->query));
    }

    /**
     * 从查询结果中获取指定数据返回
     *
     * @param array $results
     * @return array
     */
    private function getSearchResultData (array $results): array {
        $needResultsKeyDataArr = ['_scroll_id', 'hits', 'aggregations'];
        $returnData = [];
        foreach ($results as $k => $v) {
            if (!in_array($k, $needResultsKeyDataArr)) {
                continue;
            }

            $kName = preg_replace_callback('/_+([a-z])/', function ($matches) {
                return strtoupper($matches[1]);
            }, $k);
            $method = sprintf("get%sData", ucfirst($kName));
            if (!method_exists($this, $method)) {
                continue;
            }

            $returnData[$k] = call_user_func([$this, $method], $v);
        }

        return $returnData;
    }

    /**
     * 获取查询的 body 表示
     *
     * @return array
     */
    public function toQuery (): array {
        return $this->query->getGrammar()->compileSelect($this->query);
    }

    /**
     * 执行查询
     *
     * @param array $params
     * @param string $method
     * @return false|mixed
     * @throws Exception
     */
    private function runQuery (array $params, string $method = 'search') {
        try {
            return call_user_func([$this->getClient(), $method], $params);
        } catch (Exception $e) {
            throw new Exception($e->getMessage());
        } finally {
            $this->newQuery();
        }
    }

    /**
     * 游标值
     *
     * @param string $scrollId
     * @return string
     */
    private function getScrollIdData (string $scrollId): string {
        return $scrollId;
    }

    /**
     * 获取聚合的数据
     *
     * @param array $data
     * @return Collection
     */
    private function getAggregationsData (array $data): Collection {
        return collect($data);
    }

    /**
     * 获取命中数据
     *
     * @param array $data
     * @return Collection
     */
    private function getHitsData (array $data): Collection {
        return collect($data['hits'] ?? [])->map(function ($hit) {
            return $this->getOriginData($hit);
        });
    }

    /**
     * 获取元数据
     *
     * @param array $data
     * @return Collection
     */
    public function getMetaData (array $data): Collection {
        return $this->getHitsData($data['hits'] ?? []);
    }

    /**
     * 返回固定格式数据
     *
     * @param array $data
     * @return array
     */
    public function getOriginData (array $data): array {
        $mergeData = [
            '_index' => $data['_index'],
            '_type'  => $data['_type'],
            '_id'    => $data['_id'],
        ];
        if (isset($data['_score'])) {
            $mergeData['_score'] = $data['_score'];
        }

        return array_merge($data['_source'], $mergeData);
    }

    /**
     * @param string $name
     * @param array $arguments
     * @return false|mixed
     * @throws Exception
     */
    public function __call (string $name, array $arguments) {
        if (!method_exists($this->query, $name)) {
            throw new Exception(sprintf("方法：%s 不存在", $name));
        }

        $query = call_user_func_array([$this->query, $name], $arguments);
        if ($query instanceof $this->query) {
            return $this;
        }

        return $query;
    }

}