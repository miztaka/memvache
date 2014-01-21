package net.vvakame.memvache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.apphosting.api.DatastorePb.CommitResponse;
import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.GetRequest;
import com.google.apphosting.api.DatastorePb.GetResponse;
import com.google.apphosting.api.DatastorePb.GetResponse.Entity;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.apphosting.api.DatastorePb.PutResponse;
import com.google.apphosting.api.DatastorePb.Transaction;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;

/**
 * Entity単位でキャッシュに保存するStrategy.
 * バッチgetのときにアプリから利用される。
 * 
 * バッチgetはEG単位でasync呼び出しされるため、
 * GetPutCacheStrategyではmemcache呼び出しが直列化してしまい、パフォーマンスが悪い。
 * 
 * そのため、「キャッシュへの保存」のみをApiProxyで実現し、
 * 「キャッシュの利用」は上位のアプリに任せる。
 * 
 * @author vvakame
 */
public class PutCacheStrategy extends RpcVisitor {
	
	private final static Logger logger =  Logger.getLogger(PutCacheStrategy.class.getName());

	static final int PRIORITY = QueryKeysOnlyStrategy.PRIORITY + 1000;

	@Override
	public int getPriority() {
		return PRIORITY;
	}

	Map<Long, Map<Key, Entity>> putUnderTx = new HashMap<Long, Map<Key, Entity>>();
	
	/**
	 * Getを行った後の動作として、Memcacheにキャッシュを作成する。
	 * @return null
	 */
	@Override
	public byte[] post_datastore_v3_Get(GetRequest requestPb, GetResponse responsePb) {

		if (requestPb.getTransaction().hasApp()) {
			// under transaction
			return null;
		}

		// Memcacheに蓄える
		Map<Key, Entity> newMap = new HashMap<Key, Entity>();
		//List<Reference> keys = requestPb.keys();
		List<Entity> entitys = responsePb.entitys();

		for (int i = 0; i < entitys.size(); i++) {
			//Key key = PbKeyUtil.toKey(keys.get(i));
			Entity entity = entitys.get(i);
			Key key = null;
			try {
				key = PbKeyUtil.toKey(entity.getEntity().getKey());
			} catch(Exception e) {
				try {
					key = PbKeyUtil.toKey(entity.getKey());
				} catch (Exception e2) {
					logger.severe("Keyの取得に失敗しました。" + entity.toString());
				}
			}
			if (key != null) {
				newMap.put(key, entity);
			}
		}
		MemcacheService memcache = MemvacheDelegate.getMemcache();
		memcache.putAll(newMap);
		logger.fine("get from datastore size: " + newMap.size());
		
		return null;
	}

	/**
	 * Putを行った後の動作として、Memcacheにキャッシュを作成する。
	 */
	@Override
	public byte[] post_datastore_v3_Put(PutRequest requestPb, PutResponse responsePb) {
		Transaction tx = requestPb.getTransaction();
		if (tx.hasApp()) {
			// Tx下の場合はDatastoreに反映されるまで、ローカル変数に結果を保持しておく。
			final long handle = tx.getHandle();
			Map<Key, Entity> newMap = extractCache(requestPb, responsePb);
			if (putUnderTx.containsKey(handle)) {
				Map<Key, Entity> cached = putUnderTx.get(handle);
				cached.putAll(newMap);
			} else {
				putUnderTx.put(handle, newMap);
			}
		} else {
			MemcacheService memcache = MemvacheDelegate.getMemcache();
			Map<Key, Entity> newMap = extractCache(requestPb, responsePb);
			memcache.putAll(newMap);
			logger.fine("put entity to memcache: size=" + newMap.size());
		}
		return null;
	}

	private Map<Key, Entity> extractCache(PutRequest requestPb, PutResponse responsePb) {
		Map<Key, Entity> newMap = new HashMap<Key, Entity>();
		int size = requestPb.entitySize();
		List<EntityProto> entitys = requestPb.entitys();
		for (int i = 0; i < size; i++) {
			EntityProto proto = entitys.get(i);
			Reference reference = responsePb.getKey(i);
			Key key = PbKeyUtil.toKey(reference);
			Entity entity = new Entity();
			entity.setEntity(proto);
			entity.setKey(reference);
			newMap.put(key, entity);
		}
		return newMap;
	}

	/**
	 * Deleteを行う前の動作として、とりあえずMemcacheからキャッシュを削除する。
	 */
	@Override
	public Pair<byte[], byte[]> pre_datastore_v3_Delete(DeleteRequest requestPb) {
		List<Key> keys = PbKeyUtil.toKeys(requestPb.keys());
		MemcacheService memcache = MemvacheDelegate.getMemcache();
		memcache.deleteAll(keys);

		return null;
	}

	/**
	 * Commitを行った後の動作として、Putした時のキャッシュが存在していればMemcacheにキャッシュを作成する。
	 */
	@Override
	public byte[] post_datastore_v3_Commit(Transaction requestPb, CommitResponse responsePb) {
		final long handle = requestPb.getHandle();
		if (putUnderTx.containsKey(handle)) {
			Map<Key, Entity> map = putUnderTx.get(handle);
			MemvacheDelegate.getMemcache().putAll(map);
			return null;
		} else {
			return null;
		}
	}

	/**
	 * Rollbackを行った後の動作として、Putした時のキャッシュが存在していればなかった事にする。
	 */
	@Override
	public byte[] post_datastore_v3_Rollback(Transaction requestPb, CommitResponse responsePb) {
		final long handle = requestPb.getHandle();
		if (putUnderTx.containsKey(handle)) {
			putUnderTx.remove(handle);
		}
		return null;
	}
}
