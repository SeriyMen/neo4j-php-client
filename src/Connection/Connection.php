<?php

/*
 * This file is part of the GraphAware Neo4j Client package.
 *
 * (c) GraphAware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\Connection;

use GraphAware\Neo4j\Client\BoltDriver\Configuration as BoltConfiguration;
use GraphAware\Neo4j\Client\BoltDriver\Exception\MessageFailureException;
use GraphAware\Neo4j\Client\BoltDriver\GraphDatabase as BoltGraphDB;
use GraphAware\Common\Connection\BaseConfiguration;
use GraphAware\Common\Cypher\Statement;
use GraphAware\Neo4j\Client\Exception\Neo4jException;
use GraphAware\Neo4j\Client\StackInterface;

class Connection
{
    /**
     * @var string The Connection Alias
     */
    private $alias;

    /**
     * @var string
     */
    private $uri;

    /**
     * @var \GraphAware\Common\Driver\DriverInterface The configured driver
     */
    private $driver;

    /**
     * @var array
     */
    private $config;

    /**
     * @var \GraphAware\Common\Driver\SessionInterface
     */
    private $session;

    /**
     * Connection constructor.
     *
     * @param string                 $alias
     * @param string                 $uri
     * @param BaseConfiguration|null $config
     */
    public function __construct($alias, $uri, $config = null)
    {
        $this->alias = (string) $alias;
        $this->uri = (string) $uri;
        $this->config = $config;

        $this->buildDriver();
    }

    /**
     * @return string
     */
    public function getAlias()
    {
        return $this->alias;
    }

    /**
     * @return \GraphAware\Common\Driver\DriverInterface
     */
    public function getDriver()
    {
        return $this->driver;
    }

    /**
     * @param null  $query
     * @param array $parameters
     * @param null  $tag
     *
     * @return \GraphAware\Common\Driver\PipelineInterface
     */
    public function createPipeline($query = null, $parameters = [], $tag = null)
    {
        $this->checkSession();
        $parameters = is_array($parameters) ? $parameters : [];

        return $this->session->createPipeline($query, $parameters, $tag);
    }

    /**
     * @param string      $statement
     * @param array|null  $parameters
     * @param null|string $tag
     *
     * @throws Neo4jException
     *
     * @return \GraphAware\Common\Result\Result
     */
    public function run($statement, $parameters = null, $tag = null)
    {
        $this->checkSession();
        if (empty($statement)) {
            throw new \InvalidArgumentException(sprintf('Expected a non-empty Cypher statement, got "%s"', $statement));
        }
        $parameters = (array) $parameters;

        try {
            $result = $this->session->run($statement, $parameters, $tag);
            return $result;
        } catch (MessageFailureException $e) {
            $exception = new Neo4jException($e->getMessage());
            $exception->setNeo4jStatusCode($e->getStatusCode());

            throw $exception;
        }
    }

    /**
     * @param array $queue
     *
     * @return \GraphAware\Common\Result\ResultCollection
     */
    public function runMixed(array $queue)
    {
        $this->checkSession();
        $pipeline = $this->createPipeline();

        foreach ($queue as $element) {
            if ($element instanceof StackInterface) {
                foreach ($element->statements() as $statement) {
                    $pipeline->push($statement->text(), $statement->parameters(), $statement->getTag());
                }
            } elseif ($element instanceof Statement) {
                $pipeline->push($element->text(), $element->parameters(), $element->getTag());
            }
        }

        return $pipeline->run();
    }

    /**
     * @return \GraphAware\Common\Transaction\TransactionInterface
     */
    public function getTransaction()
    {
        $this->checkSession();

        return $this->session->transaction();
    }

    /**
     * @return \GraphAware\Common\Driver\SessionInterface
     */
    public function getSession()
    {
        $this->checkSession();

        return $this->session;
    }

    private function buildDriver()
    {
        $params = parse_url($this->uri);

        $uri = sprintf('%s://%s:%d', $params['scheme'], $params['host'], $params['port']);
        $config = BoltConfiguration::create()->withCredentials($params['user'], $params['pass']);
        $this->driver = BoltGraphDB::driver($uri, $config);
    }

    private function checkSession()
    {
        if (null === $this->session) {
            $this->session = $this->driver->session();
        }
    }
}
