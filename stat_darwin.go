package main

import "syscall"

func getCtime(stat *syscall.Stat_t) int64 {
	return stat.Ctimespec.Nsec + stat.Ctimespec.Sec*1000000000
}

func getMtime(stat *syscall.Stat_t) int64 {
	return stat.Mtimespec.Nsec + stat.Mtimespec.Sec*1000000000
}
