<?php

namespace Onoi\BlobStore;

use Onoi\Cache\Cache;
use InvalidArgumentException;

/**
 * Pervasive value blob store that can be used to store and retrieve key values
 * from a schema free "fast" data store such as redis. The connection to a back-end
 * is handled by the Onoi\Cache interface to support different provider solutions.
 *
 * @license GNU GPL v2+
 * @since 1.0
 *
 * @author mwjames
 */
class BlobStore {

	const INTERNALLIST = 'internal-blobstore-id-list';

	/**
	 * @var string
	 */
	private $namespace;

	/**
	 * @var string
	 */
	private $namespacePrefix;

	/**
	 * @var Cache
	 */
	private $cache;

	/**
	 * @var boolean
	 */
	private $usageState = true;

	/**
	 * @var integer
	 */
	private $expiry = 0;

	/**
	 * @since 1.0
	 *
	 * @param string $namespace
	 * @param Cache $cache
	 */
	public function __construct( $namespace, Cache $cache ) {

		if ( !is_string( $namespace ) ) {
			throw new InvalidArgumentException( "Expected the namespace to be a string" );
		}

		$this->namespace = $namespace;
		$this->cache = $cache;
	}

	/**
	 * @since 1.0
	 *
	 * @return boolean
	 */
	public function canUse() {
		return $this->usageState;
	}

	/**
	 * @since 1.0
	 *
	 * @param boolean $usageState
	 */
	public function setUsageState( $usageState ) {
		$this->usageState = (bool)$usageState;
	}

	/**
	 * @since 1.0
	 *
	 * @param string $prefix
	 */
	public function setNamespacePrefix( $prefix ) {
		$this->namespacePrefix = $prefix;
	}

	/**
	 * @since 1.0
	 *
	 * @param string $id
	 *
	 * @return boolean
	 */
	public function exists( $id ) {
		return $this->cache->contains( $this->getKey( $id ) );
	}

	/**
	 * @since 1.0
	 *
	 * @param string $id
	 *
	 * @return Container
	 */
	public function read( $id ) {

		$id = $this->getKey( $id );

		if ( $this->cache->contains( $id ) ) {
			$data = unserialize( $this->cache->fetch( $id ) );
		} else {
			$this->addToInternalList( $id );
			$data = array();
		}

		return new Container( $id, (array)$data );
	}

	/**
	 * @since 1.0
	 *
	 * @param Container $container
	 */
	public function save( Container $container ) {

		$this->cache->save(
			$container->getContainerId(),
			serialize( $container->getContainerData() ),
			$this->expiry
		);

		unset( $container );
	}

	/**
	 * @since 1.0
	 *
	 * @param string $id
	 */
	public function delete( $id ) {
		$this->removeFromInternalList( $this->getKey( $id ) );
		$this->cache->delete( $this->getKey( $id ) );
	}

	/**
	 * Drop all containers that belong to the invoked namespace at once
	 *
	 * @since 1.0
	 */
	public function drop() {

		$trackerId = $this->namespacePrefix . ':' . md5( self::INTERNALLIST );
		$container = unserialize( $this->cache->fetch( $trackerId ) );

		if ( !$container ) {
			$container = array();
		}

		foreach ( $container as $ns => $identifiers ) {

			if ( $this->namespace !== $ns ) {
				continue;
			}

			foreach ( array_keys( $identifiers ) as $id ) {
				$this->cache->delete( $id );
			}
		}
	}

	private function getKey( $id ) {

		if ( !is_string( $id ) ) {
			throw new InvalidArgumentException( "Expected the id o be a string" );
		}

		return  $this->namespacePrefix . ':' . $this->namespace . ':' . $id;
	}

	/**
	 * Track container id's separatly to be able to find them at once if required
	 */
	private function addToInternalList( $id ) {

		$internalListId = $this->namespacePrefix . ':' . md5( self::INTERNALLIST );
		$container = unserialize( $this->cache->fetch( $internalListId ) );

		if ( !$container ) {
			$container = array();
		}

		$container[$this->namespace][$id] = true;

		$this->cache->save(
			$internalListId,
			serialize( $container )
		);
	}

	private function removeFromInternalList( $id ) {

		$internalListId = $this->namespacePrefix . ':' . md5( self::INTERNALLIST );
		$container = unserialize( $this->cache->fetch( $internalListId ) );

		if ( !isset( $container[$this->namespace] ) ) {
			return;
		}

		unset( $container[$this->namespace][$id] );

		$this->cache->save(
			$internalListId,
			serialize( $container )
		);
	}

}
