<?php

require '../vendor/autoload.php';

use EsORM\EsSearch;
use Illuminate\Support\Collection;

class Demo {

    private EsSearch $esSearch;

    public function getInstance (array $config, string $index): EsSearch {
        $this->esSearch = EsSearch::getInstance($config)->index($index);
        return $this->esSearch;
    }

    /**
     * 创建es数据
     *
     * @param array $data
     * @return array
     * @throws Exception
     */
    public function create (array $data): array {
        return $this->esSearch->create($data);
    }

    /**
     * 编辑es数据
     *
     * @param string $id
     * @param array $data
     * @return bool
     * @throws Exception
     */
    public function update (string $id, array $data): bool {
        return $this->esSearch->update($id, $data);
    }

    /**
     * 查找单条数据
     *
     * @param string $id
     * @param array $columns
     * @return Collection
     * @throws Exception
     */
    public function find (string $id, array $columns): Collection {
        return $this->esSearch->find($id, $columns);
    }
}

$demo = new Demo();
$esSearch = (new Demo())->getInstance([
    'hosts' => ['es:9200'],
], 'test_index');

//$esSearch->create([
//    'name'     => 'create demo',
//    'quantity' => 100,
//]);

//$findResult = $esSearch->find('57433', ['name', 'quantity']);
$findResult = $esSearch->select('quantity')->find('57433');
var_dump($findResult);
