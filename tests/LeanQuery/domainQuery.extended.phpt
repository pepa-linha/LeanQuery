<?php

use LeanQuery\DomainQueryFactory;
use LeanQuery\Hydrator;
use LeanQuery\QueryHelper;
use Tester\Assert;

require_once __DIR__ . '/../bootstrap.php';

$queryHelper = new QueryHelper($mapper);
$hydrator = new Hydrator($connection, $mapper);
$domainQueryFactory = new DomainQueryFactory($entityFactory, $connection, $mapper, $hydrator, $queryHelper);

////////////////////

$domainQuery = $domainQueryFactory->createQuery();

$domainQuery
	->select('b')
	->from('Book', 'b')
	->sqlSelect('COUNT(*) as count')
	->sqlGroupBy('b.reviewer_id')
	->sqlOrderBy('b.name');

$books = $domainQuery->getEntities();

Assert::count(1, $queries);

$expected = "SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
	"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], " .
	"[b].[website] AS [b__website], [b].[available] AS [b__available] , COUNT(*) as count " .
	"FROM [book] AS [b] GROUP BY [b].[reviewer_id] ORDER BY [b].[name]";

Assert::equal($expected, $queries[0]);
