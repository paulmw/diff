package com.cloudera.tools.diff;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Diff {

	private FileSystem fsA;
	private FileSystem fsB;

	private FileSystem connect(String path) throws IOException {
		Configuration conf;
		if(path.startsWith("hdfs://")) {
			conf = new Configuration(false);
			String nn = path.substring(0, path.indexOf("/", 7));
			conf.set("fs.default.name", nn);
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		} else {
			conf = new Configuration();
		}
		return FileSystem.get(conf);
	}
	
	public void compare(String source, String destination) throws IOException {
		fsA = connect(source);
		fsB = connect(destination);
		Path a = new Path(source);
		Path b = new Path(destination);
		if(bothExist(a, b)) {
			FileStatus sa = fsA.getFileStatus(a);
			FileStatus sb = fsB.getFileStatus(b);
			compare1(sa, sb);
		}
	}

	private boolean bothExist(Path a, Path b) throws IOException {
		boolean aExists = fsA.exists(a);
		boolean bExists = fsB.exists(b);
		if(!aExists || !bExists) {
			if(!aExists) {
				System.out.println(a + " doesn't exist");
			}
			if(!bExists) {
				System.out.println(b + " doesn't exist");
			}
			return false;
		}
		return true;
	}

	private Map<String, FileStatus> children(FileSystem fs, Path path) throws IOException {
		Map<String, FileStatus> children = new TreeMap<String, FileStatus>();
		FileStatus [] files = fs.listStatus(path);
		for(FileStatus file : files) {
			children.put(file.getPath().getName(), file);
		}
		return children;
	}

	private void compare1(FileStatus sa, FileStatus sb) throws IOException {
		if(sa.isDir() && sb.isDir()) {
			Map<String, FileStatus> childrenOfA = children(fsA, sa.getPath());
			Map<String, FileStatus> childrenOfB = children(fsB, sb.getPath());

			Set<String> leftChildren = new TreeSet<String>(childrenOfA.keySet());
			leftChildren.removeAll(childrenOfB.keySet());
			for(String child : leftChildren) {
				System.out.println("Only in left: " + childrenOfA.get(child).getPath());	
			}			

			Set<String> rightChildren = new TreeSet<String>(childrenOfB.keySet());
			rightChildren.removeAll(childrenOfA.keySet());
			for(String child : rightChildren) {
				System.out.println("Only in right: " + childrenOfB.get(child).getPath());	
			}

			Set<String> intersection = new TreeSet<String>(childrenOfA.keySet());
			intersection.retainAll(childrenOfB.keySet());
			for(String i : intersection) {
				compare1(childrenOfA.get(i), childrenOfB.get(i));
			}

		} else if(!sa.isDir() && !sb.isDir()) {
			if(sa.getLen() != sb.getLen()) {
				System.out.println(sa.getPath() + " and " + sb.getPath() + " differ in length.");
			} else {
				FileChecksum ca = fsA.getFileChecksum(sa.getPath());
				FileChecksum cb = fsB.getFileChecksum(sb.getPath());
				if(!ca.equals(cb)) {
					System.out.println(sa.getPath() + " and " + sb.getPath() + " differ in content.");
				}
			}
		} else {
			System.out.println(sa.getPath() + " and " + sb.getPath() + " differ in type.");
		}

	}

	public static void main(String[] args) throws IOException {
		if(args.length != 2) {
			System.out.println("Usage: hadoop jar diff-*.jar com.cloudera.tools.diff.Diff <source> <destination>");
			System.exit(1);
		}
		Diff diff = new Diff();
		diff.compare(args[0], args[1]);
	}

}
