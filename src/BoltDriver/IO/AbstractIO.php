<?php

/*
 * This file is part of the GraphAware Bolt package.
 *
 * (c) Graph Aware Limited <http://graphaware.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GraphAware\Neo4j\Client\BoltDriver\IO;

use GraphAware\Neo4j\Client\BoltDriver\Exception\IOException;

abstract class AbstractIO implements IoInterface
{
    /**
     * @return bool
     *
     * @throws IOException
     */
    public function assertConnected()
    {
        if (!$this->isConnected()) {
            return $this->connect();
        }

        return true;
    }
}
