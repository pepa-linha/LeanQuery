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

$domainQuery->select('b')
		->from('Book', 'b')
		->join('b.author', 'a')->select('a')
		->leftJoin('b.tags', 't')->select('t');

$books = $domainQuery->getEntities();

$output = '';

foreach ($books as $book) {
	$output .= "$book->name\r\n";
	$output .= "\tAuthor: {$book->author->name}\r\n";
	$output .= "\tTags:\r\n";
	foreach ($book->tags as $tag) {
		$output .= "\t\t$tag->name\r\n";
	}
}

Assert::count(1, $queries);

$expected =
		"SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
		"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], [b].[website] AS [b__website], " .
		"[b].[available] AS [b__available] , [a].[id] AS [a__id], [a].[name] AS [a__name], [a].[web] AS [a__web] , [t].[id] AS [t__id], " .
		"[t].[name] AS [t__name] , [book_tag1].[id] AS [book_tag1__id], [book_tag1].[book_id] AS [book_tag1__book_id], " .
		"[book_tag1].[tag_id] AS [book_tag1__tag_id] " .
		"FROM [book] AS [b] " .
		"JOIN [author] AS [a] ON [b].[author_id] = [a].[id] " .
		"LEFT JOIN [book_tag] AS [book_tag1] ON [b].[id] = [book_tag1].[book_id] " .
		"LEFT JOIN [tag] AS [t] ON [book_tag1].[tag_id] = [t].[id]";

Assert::equal($expected, $queries[0]);

$expected =
"The Pragmatic Programmer\r\n" .
"	Author: Andrew Hunt\r\n" .
"	Tags:\r\n" .
"		popular\r\n" .
"		ebook\r\n" .
"The Art of Computer Programming\r\n" .
"	Author: Donald Knuth\r\n" .
"	Tags:\r\n" .
"Refactoring: Improving the Design of Existing Code\r\n" .
"	Author: Martin Fowler\r\n" .
"	Tags:\r\n" .
"		ebook\r\n" .
"Introduction to Algorithms\r\n" .
"	Author: Thomas H. Cormen\r\n" .
"	Tags:\r\n" .
"		popular\r\n" .
"UML Distilled\r\n" .
"	Author: Martin Fowler\r\n" .
"	Tags:\r\n"
;

Assert::equal($expected, $output);

////////////////////

$queries = array();

$domainQuery = $domainQueryFactory->createQuery();

$domainQuery->select('b, a')
		->from('Book', 'b')
		->join('b.author', 'a')
		->join('b.tags', 't');

$domainQuery->getEntities();
$domainQuery->getEntities(); // testing cache
$books = $domainQuery->getEntities();

$output = '';

foreach ($books as $book) {
	$output .= "$book->name\r\n";
	$output .= "\tAuthor: {$book->author->name}\r\n";
}

Assert::count(1, $queries);

$expected =
		"SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
		"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], [b].[website] AS [b__website], " .
		"[b].[available] AS [b__available] , [a].[id] AS [a__id], [a].[name] AS [a__name], [a].[web] AS [a__web] " .
		"FROM [book] AS [b] " .
		"JOIN [author] AS [a] ON [b].[author_id] = [a].[id] " .
		"JOIN [book_tag] AS [book_tag1] ON [b].[id] = [book_tag1].[book_id] " .
		"JOIN [tag] AS [t] ON [book_tag1].[tag_id] = [t].[id]";

Assert::equal($expected, $queries[0]);

$expected =
"The Pragmatic Programmer\r\n" .
"	Author: Andrew Hunt\r\n" .
"Refactoring: Improving the Design of Existing Code\r\n" .
"	Author: Martin Fowler\r\n" .
"Introduction to Algorithms\r\n" .
"	Author: Thomas H. Cormen\r\n"
;

Assert::equal($expected, $output);

////////////////////

$queries = array();

$domainQuery = $domainQueryFactory->createQuery();

$domainQuery->from('Book', 'b')->select('b')
		->join('b.author', 'a')
		->join('b.tags', 't');

$books = $domainQuery->getEntities();

$output = '';

foreach ($books as $book) {
	$output .= "$book->name\r\n";
	$output .= "\tAuthor: {$book->author->name}\r\n";
	$output .= "\tTags:\r\n";
	foreach ($book->tags as $tag) {
		$output .= "\t\t$tag->name\r\n";
	}
}

Assert::count(4, $queries);

$expected = array(
	"SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
	"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], " .
	"[b].[website] AS [b__website], [b].[available] AS [b__available] " .
	"FROM [book] AS [b] " .
	"JOIN [author] AS [a] ON [b].[author_id] = [a].[id] " .
	"JOIN [book_tag] AS [book_tag1] ON [b].[id] = [book_tag1].[book_id] " .
	"JOIN [tag] AS [t] ON [book_tag1].[tag_id] = [t].[id]",

	"SELECT [author].* FROM [author] WHERE [author].[id] IN (1, 3, 5)",
	"SELECT [book_tag].* FROM [book_tag] WHERE [book_tag].[book_id] IN (1, 3, 4)",
	"SELECT [tag].* FROM [tag] WHERE [tag].[id] IN (1, 2)",
);

Assert::equal($expected, $queries);

$expected =
"The Pragmatic Programmer\r\n" .
"	Author: Andrew Hunt\r\n" .
"	Tags:\r\n" .
"		popular\r\n" .
"		ebook\r\n" .
"Refactoring: Improving the Design of Existing Code\r\n" .
"	Author: Martin Fowler\r\n" .
"	Tags:\r\n" .
"		ebook\r\n" .
"Introduction to Algorithms\r\n" .
"	Author: Thomas H. Cormen\r\n" .
"	Tags:\r\n" .
"		popular\r\n"
;

Assert::equal($expected, $output);

////////////////////

$queries = array();

$domainQuery = $domainQueryFactory->createQuery();

$domainQuery->select('b')
	->from('Book', 'b')
	->join('b.author', 'a')->select('a')
	->leftJoin('b.tags', 't')->select('t')
	->orderBy('a.name')
	->orderBy('t.name');

$books = $domainQuery->getEntities();

$output = '';

foreach ($books as $book) {
	$output .= "$book->name\r\n";
	$output .= "\tAuthor: {$book->author->name}\r\n";
	$output .= "\tTags:\r\n";
	foreach ($book->tags as $tag) {
		$output .= "\t\t$tag->name\r\n";
	}
}

Assert::count(1, $queries);

$expected =
	"SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
	"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], [b].[website] AS [b__website], " .
	"[b].[available] AS [b__available] , [a].[id] AS [a__id], [a].[name] AS [a__name], [a].[web] AS [a__web] , " .
	"[t].[id] AS [t__id], [t].[name] AS [t__name] , [book_tag1].[id] AS [book_tag1__id], [book_tag1].[book_id] AS [book_tag1__book_id], " .
	"[book_tag1].[tag_id] AS [book_tag1__tag_id] " .
	"FROM [book] AS [b] " .
	"JOIN [author] AS [a] ON [b].[author_id] = [a].[id] " .
	"LEFT JOIN [book_tag] AS [book_tag1] ON [b].[id] = [book_tag1].[book_id] " .
	"LEFT JOIN [tag] AS [t] ON [book_tag1].[tag_id] = [t].[id] " .
	"ORDER BY [a].[name] , [t].[name]";

Assert::equal($expected, $queries[0]);

$expected =
"The Pragmatic Programmer\r\n" .
"	Author: Andrew Hunt\r\n" .
"	Tags:\r\n" .
"		ebook\r\n" .
"		popular\r\n" .
"The Art of Computer Programming\r\n" .
"	Author: Donald Knuth\r\n" .
"	Tags:\r\n" .
"UML Distilled\r\n" .
"	Author: Martin Fowler\r\n" .
"	Tags:\r\n" .
"Refactoring: Improving the Design of Existing Code\r\n" .
"	Author: Martin Fowler\r\n" .
"	Tags:\r\n" .
"		ebook\r\n" .
"Introduction to Algorithms\r\n" .
"	Author: Thomas H. Cormen\r\n" .
"	Tags:\r\n" .
"		popular\r\n"
;

Assert::equal($expected, $output);

////////////////////

$queries = array();

$domainQuery = $domainQueryFactory->createQuery();

$domainQuery->select('b, a')
	->from('Book', 'b')
	->join('b.author', 'a')
	->where('a.name IN %in AND b.available = %b AND %sql', array('Martin Fowler', 'Thomas H. Cormen'), true, '1 = 1', 'a.name != ? AND b.name != "b.name"', 'a.name');

$books = $domainQuery->getEntities();

$output = '';

foreach ($books as $book) {
	$output .= "$book->name\r\n";
	$output .= "\tAuthor: {$book->author->name}\r\n";
}

Assert::count(1, $queries);

$expected =
	"SELECT [b].[id] AS [b__id], [b].[author_id] AS [b__author_id], [b].[reviewer_id] AS [b__reviewer_id], " .
	"[b].[name] AS [b__name], [b].[pubdate] AS [b__pubdate], [b].[description] AS [b__description], [b].[website] AS [b__website], " .
	"[b].[available] AS [b__available] , [a].[id] AS [a__id], [a].[name] AS [a__name], [a].[web] AS [a__web] " .
	"FROM [book] AS [b] " .
	"JOIN [author] AS [a] ON [b].[author_id] = [a].[id] " .
	"WHERE [a].[name] IN ('Martin Fowler', 'Thomas H. Cormen') AND [b].[available] = 1 AND 1 = 1 AND [a].[name] != 'a.name' AND [b].[name] != 'b.name'";

Assert::equal($expected, $queries[0]);

$expected =
"Refactoring: Improving the Design of Existing Code\r\n" .
"	Author: Martin Fowler\r\n" .
"Introduction to Algorithms\r\n" .
"	Author: Thomas H. Cormen\r\n"
;

Assert::equal($expected, $output);
