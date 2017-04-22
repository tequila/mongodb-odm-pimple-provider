<?php

namespace Tequila\Silex\Provider;

use Pimple\Container;
use Pimple\ServiceProviderInterface;
use Tequila\MongoDB\Client;
use Tequila\MongoDB\ODM\BulkWriteBuilderFactory;
use Tequila\MongoDB\ODM\DefaultMetadataFactory;
use Tequila\MongoDB\ODM\DefaultRepositoryFactory;
use Tequila\MongoDB\ODM\DocumentManager;
use Tequila\MongoDB\ODM\QueryListener\SetBulkWriteBuilderListener;
use Tequila\Silex\Provider\MongoDBServiceProvider;

class ODMServiceProvider implements ServiceProviderInterface
{
    public function register(Container $app)
    {
        if (!isset($app['mongodb.db']) || !class_exists(MongoDBServiceProvider::class)) {
            throw new \LogicException(
                sprintf(
                    '%s can only be used if %s is registered.',
                    self::class,
                    MongoDBServiceProvider::class
                )
            );
        }

        $app['mongodb.connections.options_initializer']();
        foreach ($app['mongodb.options.connections'] as $connectionName => $connectionOptions) {
            $metadataFactoryServiceId = sprintf('mongodb.odm.metadata_factory.%s', $connectionName);
            $app[$metadataFactoryServiceId] = function () {
                return new DefaultMetadataFactory();
            };

            $bulkBuilderFactoryServiceId = sprintf('mongodb.odm.bulk_builder_factory.%s', $connectionName);
            $app[$bulkBuilderFactoryServiceId] = function (Container $app) use ($connectionName) {
                /** @var Client $client */
                $client = $app['mongodb.clients'][$connectionName];

                return new BulkWriteBuilderFactory($client->getManager());
            };

            $repositoryFactoryServiceId = sprintf('mongodb.odm.repository_factory.%s', $connectionName);
            $app[$repositoryFactoryServiceId] = function (Container $app) use ($metadataFactoryServiceId) {
                return new DefaultRepositoryFactory($app[$metadataFactoryServiceId]);
            };
        }

        $app['mongodb.dbs.options_initializer']();
        foreach ($app['mongodb.options.dbs'] as $dbName => $dbOptions) {
            $dmServiceId = sprintf('mongodb.odm.dm.%s', $dbName);
            $connectionName = $dbOptions['connection'];
            $app[$dmServiceId] = function (Container $app) use ($dbName, $connectionName) {
                /** @var Client $client */
                $client = $app['mongodb.clients'][$connectionName];
                /** @var BulkWriteBuilderFactory $bulkBuilderFactory */
                $bulkBuilderFactory = $app[sprintf('mongodb.odm.bulk_builder_factory.%s', $connectionName)];
                $listener = new SetBulkWriteBuilderListener($bulkBuilderFactory);
                $client->getManager()->addQueryListener($listener);

                return new DocumentManager(
                    $app['mongodb.dbs'][$dbName],
                    $bulkBuilderFactory,
                    $app[sprintf('mongodb.odm.repository_factory.%s', $connectionName)],
                    $app[sprintf('mongodb.odm.metadata_factory.%s', $connectionName)]
                );
            };
        }

        $app['mongodb.odm.dm'] = function (Container $app) {
            $dmServiceId = sprintf('mongodb.odm.dm.%s', $app['mongodb.config.default_db_name']);
            return $app[$dmServiceId];
        };
    }
}