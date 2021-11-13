package main

import "syscall"

func getCtime(stat *syscall.Stat_t) int64 {
	return stat.Ctim.Nsec + stat.Ctim.Sec*1000000000
}

func getMtime(stat *syscall.Stat_t) int64 {
	return stat.Mtim.Nsec + stat.Mtim.Sec*1000000000
}
