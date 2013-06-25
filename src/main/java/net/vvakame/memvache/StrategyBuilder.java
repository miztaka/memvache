package net.vvakame.memvache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StrategyBuilder {
	
	private Map<String,List<Class<? extends Strategy>>> strategyConfig = new HashMap<String,List<Class<? extends Strategy>>>();
	
	public static StrategyBuilder newBuilder() {
		return new StrategyBuilder();
	}
	private StrategyBuilder() {}
	
	public StrategyBuilder addStrategy(String serviceName, Class<? extends Strategy> cls) {
		if (! strategyConfig.containsKey(serviceName)) {
			strategyConfig.put(serviceName, new ArrayList<Class<? extends Strategy>>());
		}
		strategyConfig.get(serviceName).add(cls);
		return this;
	}
	
	public Map<String,List<Class<? extends Strategy>>> buid() {
		return strategyConfig;
	}

}
