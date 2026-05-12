package util

import "dfs/messages"

func MapArrayAddressesToNodeInfo(addresses []string) []*messages.NodeInfo {
	nodes := make([]*messages.NodeInfo, len(addresses))
	for i, addr := range addresses {
		nodes[i] = &messages.NodeInfo{Address: addr}
	}
	return nodes
}
