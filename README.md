# Elasticsearch ORM

类似ORM一样操作Elasticsearch curd

## Version
|                             |         version       |
| --------------------------- | --------------------- |
| PHP                         |          ^7.4         |
| elasticsearch/elasticsearch |          ~7.0         |
| illuminate/support          | ^5.3 ^6.0 ^7.0 ^8.0   |

## Install
```
composer require th-zhou/esorm
```

## QuickStart
### getInstance
```php
$esSearch = EsSearch::getInstance([
    'hosts' => ['es:9200'],
]);
```
### create
```php
$esSearch->index('demo')->create([
    'name'     => 'create demo',
    'quantity' => 100,
]);
```
### update
```php
$esSearch->index('demo')->update('100', [
    'name' => 'update demo'
]);
```
### delete
```php
$esSearch->index('demo')->delete('100');
```
### find
```php
$esSearch->index('demo')->find('100');
```
```php
$esSearch->index('demo')->find('100', ['name']);
$esSearch->index('demo')->select(['name'])->find('100');
```
### first
```php
$esSearch->index('demo')->first();
$esSearch->index('demo')->first(['name']);
```
### get
```php
$esSearch->index('demo')->get();
$esSearch->index('demo')->get(['name']);
$esSearch->index('demo')->get(['name'], true); // true return total
```
### paginate
```php
$page = 1;
$pageSize = 10;
$esSearch->index('demo')->paginate($page, $pageSize);
```
### chunk
```php
$esSearch->index('demo')->chunk(function ($data) {
    // do
});

or

$limit = 1000;
$scroll = '5m';
$esSearch->index('demo')->chunk(function ($data) {
    // do
}, $limit, $scroll);
```
### toQuery
不执行任务es操作，只会返回即将操作的参数。

### more
#### where
同系列的还有orWhere
```php
$esSearch->index('demo')->where(function (\EsORM\Query $query) {
    $query->where('name', 'create_demo')->orWhere('quantity', 100);
});

$esSearch->index('demo')->where('name', 'create_demo');

$esSearch->index('demo')->where('quantity' '>=', 100);

$esSearch->index('demo')->where(['name' => 'create_demo', 'quantity' => 100]);

$esSearch->index('demo')->where(['name' => 'create_demo', 'quantity' => [50, 150]]);
```
### whereMatch
分词匹配查询。同系列的还有orWhereMatch、whereNotMatch、orWhereNotMatch
```php
$esSearch->index('demo')->whereMatch('name', 'create_demo');
```
### whereMatchKeyword
不分词匹配查询。同系列的还有orWhereMatchKeyword、whereNotMatchKeyword、orWhereNotMatchKeyword
```php
$esSearch->index('demo')->whereMatchKeyword('name', 'create_demo');
```
### whereTerm
精确匹配查询。同系列的还有orWhereTerm、whereNotTerm、orWhereNotTerm
```php
$esSearch->index('demo')->whereTerm('name', 'create_demo');
```
### whereTermKeyword
不分词精确匹配查询。同系列的还有orWhereTermKeyword、whereNotTermKeyword、orWhereNotTermKeyword
```php
$esSearch->index('demo')->whereTermKeyword('name', 'create_demo');
```
### 多个值精确匹配查询
#### whereTerms
同系列的还有orWhereTerms、whereNotTerms、orWhereNotTerms
```php
$esSearch->index('demo')->whereTerms('quantity', [100, 200]);
```
#### whereIn
同系列的还有orWhereIn、whereNotIn、orWhereNotIn
```php
$esSearch->index('demo')->whereIn('quantity', [100, 200]);
```
### whereRange
同系列的还有orWhereRange
```php
$esSearch->index('demo')->whereRange('quantity', [100, 200]);
```
### whereBetween
同系列的还有orWhereBetween、whereNotBetween、orWhereNotBetween
```php
$esSearch->index('demo')->whereBetween('quantity', [100, 200]);
```
### whereExists
验证字段是否存在。同系列还有orWhereExists、whereNotExists、orWhereNotExists
```php
$esSearch->index('demo')->whereExists('name');
```
### select
查询指定字段
```php
$esSearch->index('demo')->select('name', 'quantity');

or

$esSearch->index('demo')->select(['name', 'quantity']);
```
### orderBy
```php
$esSearch->index('demo')->orderBy('quantity', 'desc'); // asc
```
### offset
```php
$esSearch->index('demo')->offset(0);
```
### limit
```php
$esSearch->index('demo')->limit(100);
```
### aggBy
聚合操作
```php
$esSearch->index('demo')->aggBy('quantity', 'sum');

or

$esSearch->index('demo')->aggBy(['single_avg_quantity' => ['avg' => ['field' => 'quantity']]]);

or

$esSearch->index('demo')->aggBy([['quantitys' => ['terms' => ['field' => 'quantity']]]])

or

$esSearch->index('demo')->aggBy([['single_avg_quantity' => ['avg' => ['field' => 'quantity']]], ['quantitys' => ['terms' => ['field' => 'quantity']]]]);
```
