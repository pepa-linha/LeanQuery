<?php

namespace LeanQuery;

use ArrayObject;
use LeanMapper\Connection;
use LeanMapper\Entity;
use LeanMapper\Exception\InvalidArgumentException;
use LeanMapper\Exception\InvalidMethodCallException;
use LeanMapper\Exception\InvalidStateException;
use LeanMapper\Fluent;
use LeanMapper\IEntityFactory;
use LeanMapper\IMapper;
use LeanMapper\Result;
use LeanMapper\Row;
use stdClass;

/**
 * @author Vojtěch Kohout
 */
class DomainQuery
{

	const PATTERN_IDENTIFIER = '[a-zA-Z0-9_\x7f-\xff]+'; // TODO: move to separate class in Lean Mapper

	const JOIN_TYPE_INNER = 'join';

	const JOIN_TYPE_LEFT = 'leftJoin';

	const ORDER_ASC = 'ASC';

	const ORDER_DESC = 'DESC';

	/** @var IEntityFactory */
	private $entityFactory;

	/** @var Connection */
	private $connection;

	/** @var IMapper */
	private $mapper;

	/** @var Hydrator */
	private $hydrator;

	/** @var QueryHelper */
	private $queryHelper;

	/** @var DomainQueryHelper */
	private $domainQueryHelper;

	/** @var Aliases */
	private $aliases;

	/** @var HydratorMeta */
	private $hydratorMeta;

	/** @var stdClass */
	private $clauses;

	/** @var ArrayObject */
	private $relationshipTables;

	/** @var array */
	private $results;

	/** @var Entity[] */
	private $entities;

	/** @var stdClass */
	private $sqlClauses;

	/** @var int */
	private $offset;

	/** @var int */
	private $limit;

	/**
	 * @param IEntityFactory $entityFactory
	 * @param Connection $connection
	 * @param IMapper $mapper
	 * @param Hydrator $hydrator
	 * @param QueryHelper $queryHelper
	 */
	public function __construct(IEntityFactory $entityFactory, Connection $connection, IMapper $mapper, Hydrator $hydrator, QueryHelper $queryHelper)
	{
		$this->entityFactory = $entityFactory;
		$this->connection = $connection;
		$this->mapper = $mapper;
		$this->hydrator = $hydrator;
		$this->queryHelper = $queryHelper;

		$this->aliases = new Aliases;
		$this->hydratorMeta = new HydratorMeta;
		$this->clauses = (object) array(
			'select' => array(),
			'from' => null,
			'join' => array(),
			'where' => array(),
			'orderBy' => array(),
		);

		$this->sqlClauses = (object) array(
			'select' => array(),
			'groupBy' => array(),
			'orderBy' => array(),
			'having' => array(),
		);

		$this->relationshipTables = new ArrayObject;
		$this->domainQueryHelper = new DomainQueryHelper($mapper, $this->aliases, $this->hydratorMeta, $this->clauses, $this->relationshipTables);
	}

	/**
	 * @param string $aliases
	 * @throws InvalidArgumentException
	 * @return $this
	 */
	public function select($aliases)
	{
		$this->cleanCache();

		if (!preg_match('#^\s*(' . self::PATTERN_IDENTIFIER . '\s*,\s*)*(' . self::PATTERN_IDENTIFIER . ')\s*$#', $aliases)) {
			throw new InvalidArgumentException;
		}
		$this->clauses->select += array_fill_keys(preg_split('#\s*,\s*#', trim($aliases)), true);

		return $this;
	}

	/**
	 * @param $select
	 * @return $this
	 */
	public function sqlSelect($select)
	{
		$this->cleanCache();

		$this->sqlClauses->select += array(func_get_args());
		return $this;
	}

	/**
	 * @param string $entityClass
	 * @param string $alias
	 * @throws InvalidMethodCallException
	 * @return $this
	 */
	public function from($entityClass, $alias)
	{
		$this->cleanCache();

		if ($this->clauses->from !== null) {
			throw new InvalidMethodCallException('Clause FROM was already defined.');
		}
		$this->domainQueryHelper->setFrom($entityClass, $alias);

		return $this;
	}

	/**
	 * @param string $definition
	 * @param string $alias
	 * @param string $onCondition
	 * @return $this
	 */
    public function join($definition, $alias, $onCondition = null)
	{
		$this->cleanCache();

		$this->domainQueryHelper->addJoinByType($definition, $alias, self::JOIN_TYPE_INNER, array_slice(func_get_args(), 2));
		return $this;
	}

	/**
	 * @param string $definition
	 * @param string $alias
 	 * @param string $onCondition
	 * @return $this
	 */
    public function leftJoin($definition, $alias, $onCondition = null)
	{
		$this->cleanCache();

		$this->domainQueryHelper->addJoinByType($definition, $alias, self::JOIN_TYPE_LEFT, array_slice(func_get_args(), 2));
		return $this;
	}

	/**
	 * @param $args
	 * @return $this
	 */
	public function where($args)
	{
		$this->cleanCache();
		$this->domainQueryHelper->addWhere(func_get_args());

		return $this;
	}

	/**
	 * @param string $property
	 * @param string $direction
	 * @return $this
	 * @throws InvalidArgumentException
	 */
	public function orderBy($property, $direction = self::ORDER_ASC)
    {
	    $this->cleanCache();

		$this->domainQueryHelper->addOrderBy($property, $direction);
		return $this;
	}

	public function sqlOrderBy($column, $direction = self::ORDER_ASC)
	{
		$this->cleanCache();

		$this->sqlClauses->orderBy += array(func_get_args());
		return $this;
	}

	public function sqlGroupBy($column)
	{
		$this->cleanCache();

		$this->sqlClauses->groupBy += array(func_get_args());
		return $this;
	}

	public function sqlHaving($args)
	{
		$this->cleanCache();

		$this->sqlClauses->having[] = func_get_args();
		return $this;
	}

	/**
	 * @return Fluent
	 * @throws InvalidStateException
	 */
	public function createFluent()
	{
		if ($this->clauses->from === null or empty($this->clauses->select)) {
			throw new InvalidStateException('You have to specify SELECT and FROM clauses.');
		}
		$statement = $this->connection->command();

		foreach (array_keys($this->clauses->select) as $alias) { // SELECT
			$statement->select(
				$this->queryHelper->formatSelect(
					$this->domainQueryHelper->getReflection($this->aliases->getEntityClass($alias)),
					$alias
				)
			);
			if (array_key_exists($alias, $this->relationshipTables)) {
				call_user_func_array(
					array($statement, 'select'),
					array_merge(array('%n.%n AS %n, %n.%n AS %n, %n.%n AS %n'), $this->relationshipTables[$alias])
				);
			}
		}

		// (native) SQL SELECT
		foreach ($this->sqlClauses->select as $sqlSelect) {
			call_user_func_array(array($statement, 'select'), $sqlSelect);
		}

		$statement->from(array($this->clauses->from['table'] => $this->clauses->from['alias'])); // FROM

		foreach ($this->clauses->join as $join) { // JOIN
			call_user_func_array(
				array($statement, $join['type']),
				array_merge(array('%n AS %n'), $join['joinParameters'])
			);
			call_user_func_array(
				array($statement, 'on'),
				array_merge(array('%n.%n = %n.%n'), $join['onParameters'])
			);

			if (isset($join['customOnParameters']) && is_array($join['customOnParameters']) && count($join['customOnParameters']) > 0) {
				call_user_func_array(
					array($statement, 'and'), array_values($join['customOnParameters'][0])
				);
			}
		}

		if (!empty($this->clauses->where)) { // WHERE
			call_user_func_array(array($statement, 'where'), $this->clauses->where);
		}

		foreach ($this->clauses->orderBy as $orderBy) { // ORDER BY
			$statement->orderBy('%n.%n', $orderBy[0], $orderBy[1]);
			if ($orderBy[2] === self::ORDER_DESC) {
				$statement->desc();
			}
		}

		// (native) SQL GROUP BY
		foreach ($this->sqlClauses->groupBy as $sqlGroupBy) {
			call_user_func_array(array($statement, 'groupBy'), $sqlGroupBy);
		}

		// (native) SQL HAVING
		$havingApplied = false;
		foreach ($this->sqlClauses->having as $sqlHaving) {
			call_user_func_array(array($statement, $havingApplied === false ? 'having' : 'and'), $sqlHaving);
			$havingApplied = true;
		}

		// (native) SQL ORDER
		foreach ($this->sqlClauses->orderBy as $sqlOrder) {
			call_user_func_array(array($statement, 'orderBy'), $sqlOrder);
		}

		if ($this->offset) {
			$statement->offset($this->offset);
		}

		if ($this->limit) {
			$statement->limit($this->limit);
		}

		return $statement;
	}

	/**
	 * @param string $alias
	 * @throws InvalidArgumentException
	 * @return Result
	 */
	public function getResult($alias)
	{
		if ($this->results === null) {
			$relationshipFilter = array_keys($this->clauses->select);
			foreach ($relationshipFilter as $filteredAlias) {
				if (array_key_exists($filteredAlias, $this->relationshipTables)) {
					$relationshipFilter[] = $this->relationshipTables[$filteredAlias][0];
				}
			}
			$this->results = $this->hydrator->buildResultsGraph(
				$this->createFluent()->execute()->setRowClass(null)->fetchAll(),
				$this->hydratorMeta,
				$relationshipFilter
			);
		}
		if (!array_key_exists($alias, $this->results)) {
			throw new InvalidArgumentException;
		}
		return $this->results[$alias];
	}

	/**
	 * @return Entity[]
	 */
	public function getEntities()
	{
		if ($this->entities === null) {
			$entities = array();
			$entityClass = $this->clauses->from['entityClass'];
			$primaryKey = $this->mapper->getPrimaryKey($this->mapper->getTable($entityClass));
			$result = $this->getResult($this->clauses->from['alias']);
			foreach ($result as $key => $row) {
				$entity = $this->entityFactory->createEntity($entityClass, new Row($result, $key));
				$entities[$entity->{$primaryKey}] = $entity;
				$entity->makeAlive($this->entityFactory, $this->connection, $this->mapper);
			}
			$this->entities = $this->entityFactory->createCollection($entities);
		}
		return $this->entities;
	}

	/**
	 * @return Entity|null
	 */
	public function getEntity()
	{
		$entities = $this->getEntities();
		return ($entity = reset($entities)) !== false ? $entity : null;
	}

	/**
	 * @param int|null $limit
	 * @return $this
	 */
	public function limit($limit = null)
	{
		$this->limit = $limit !== null ? (int) $limit : null;
		return $this;
	}

	/**
	 * @param int|null $offset
	 * @return $this
	 */
	public function offset($offset = null)
	{
		$this->offset = $offset !== null ? (int) $offset : null;
		return $this;
	}

	////////////////////
	////////////////////

	private function cleanCache()
	{
		$this->results = $this->entities = null;
	}

	/**
	 * Helper method to determine whether a SQL subquery for COUNT(*) is needed
	 * @return bool
	 */
	public function needSubqueryForCount()
	{
		return !empty($this->sqlClauses->groupBy);
	}
}
