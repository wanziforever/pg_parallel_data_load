package main

func sizeConvert(l int64) (int64, string) {
	var sizes []string = []string{ "B", "KB", "MB", "GB", "TB" };
	var order = 0;
	for l >= 1024 && order < len(sizes) - 1 {
    order++;
    l = l/1024;
	}
	return l, sizes[order]
}
