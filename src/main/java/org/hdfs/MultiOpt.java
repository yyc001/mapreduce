package org.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MultiOpt {
    FileSystem fs;
    public static final String HDFS_PATH = "hdfs://tsxtSandbox:9000";

    public static void main(String[] args) throws Exception {
        MultiOpt self = new MultiOpt();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.replication", "1");
        self.fs = FileSystem.get(new URI(HDFS_PATH), conf);
        //todo args length restriction
        try {
            switch (args[0]) {
                case "list":
                    self.listFiles(args);
                    break;
                case "mkdir":
                    self.mkdir(args);
                    break;
                case "create":
                    self.create(args);
                    break;
                case "delete":
                    self.delete(args);
                    break;
                case "upload":
                    self.upload(args);
                    break;
                case "down":
                    self.download(args);
                    break;
                case "cat":
                    self.cat(args);
                    break;
                case "ls":
                    self.lsf(args);
                    break;
                case "append":
                    self.append(args);
                    break;
                case "mv":
                    self.mv(args);
                    break;
                case "help":
                    System.out.println("list mkdir upload help ......");
                    break;
                default:
                    System.err.println("No matching commands");
            }
        } catch (Exception e) {
            // 不知道这样有没有用
            self.fs.close();
            e.printStackTrace();
        }
    }

    void listFiles(String[] args) throws IOException {
        String dir = args[1];

        //2:调用方法listFiles 获取 /目录下所有的文件信息，，参数true代表递归遍历
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(dir), true);

        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            FsPermission fp = fileStatus.getPermission();
            String permission = fp.toString();
            long length = fileStatus.getLen();
            long modifyTime = fileStatus.getModificationTime();
            Path path = fileStatus.getPath();

            //给定 HDFS 中某一个目录，递归输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息;
            System.out.printf("%s\n %s\t%d\t%d\t%n\n", path, permission, length, modifyTime);

        }

    }

    void mkdir(String[] args) throws IOException {
        String dir = args[1];
        Path path = new Path(dir);
        if (fs.exists(path)) {
            System.out.println("Directory '" + dir + "' have already exists.");
        } else {
            fs.mkdirs(path);
        }
    }

    void create(String[] args) throws IOException {
        Path path = new Path(args[1]);
        if(fs.exists(path)) {
            System.out.println("'" + path + "' have already exists.");
            return;
        }
        if(fs.exists(path.getParent()) && fs.getFileStatus(path.getParent()).isFile()) {
            System.out.println("'" + path + "' is a file, not a directory.");
            return;
        } else {
            fs.mkdirs(path.getParent());
        }
        fs.create(path);
    }

    void delete(String[] args) throws IOException {
        //  提供一个 HDFS 内的文件的路径，对该文件进行删除操作
        //  删除目录时，当该目录为空时删除，当该目录不为空时不删除该目录；
        Path path = new Path(args[1]);
        if(fs.exists(path)) {
            if(fs.getFileStatus(path).isFile()) {
                fs.delete(path, true);
            } else {
                if(fs.listStatus(path).length == 0) {
                    fs.delete(path, true);
                } else {
                    System.out.println("'" + path + "' is not empty");
                }
            }
        }
    }

    void upload(String[] args) throws IOException {
        Path localFilePath = new Path(args[1]);
        Path hdfsDir = new Path(args[2]);
        //append or overwrite
        Path aimFile = Path.mergePaths(hdfsDir, new Path(localFilePath.getName()));
        System.out.println(aimFile);
        //java.lang.IllegalArgumentException: Wrong FS: hdfs://install.sh, expected: hdfs://tsxtSandbox:9000
        if(fs.exists(aimFile)) {
            if(fs.getFileStatus(aimFile).isDirectory()) {
                System.out.println("'" + aimFile + "' is a directory.");
                return;
            }
            System.out.print("append or overwrite:");
            Scanner scanner = new Scanner(System.in);
            String opt = scanner.nextLine();
            if(opt.startsWith("a")) {
//                System.out.println("append");
                FileInputStream local = new FileInputStream(args[1]);
                FSDataOutputStream stream = fs.append(aimFile);
                byte[] buf = new byte[4096];
                int len = -1;
                while((len = local.read(buf)) > 0) {
                    stream.write(buf, 0, len);
//                    System.out.write(buf, 0, len);
                }
//                stream.write("hello".getBytes());
                // why ???
                stream.flush();
                stream.close();
            } else {
                fs.copyFromLocalFile(false, true, localFilePath, hdfsDir);
            }
        } else {
            fs.copyFromLocalFile(false, true, localFilePath, hdfsDir);
        }
    }

    void download(String[] args) throws IOException {
        Path hdfsFilePath = new Path(args[1]);
        Path localDir = new Path(args[2]);
        //todo save as new when duplicate
        if(new File(localDir + "/" + hdfsFilePath.getName()).exists()) {
            //todo
        }
        // org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "file"
        fs.copyToLocalFile(false, hdfsFilePath, localDir);
    }

    void cat(String[] args) throws IOException {
        String hdfsFilePath = args[1];
        FSDataInputStream inputStream = fs.open(new Path(hdfsFilePath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        while ((str = reader.readLine()) != null) {
            System.out.println(str);
        }
        System.out.println();
    }

    void lsf(String[] args) throws IOException {
        String hdfsFilePath = args[1];
        FileStatus fileStatus = fs.getFileStatus(new Path(hdfsFilePath));
        FsPermission fp = fileStatus.getPermission();
        String permission = fp.toString();
        long length = fileStatus.getLen();
        long modifyTime = fileStatus.getModificationTime();
        Path path = fileStatus.getPath();
        //显示 HDFS 中指定的文件的读写权限、大小、创建时间、路径等信息；
        System.out.printf("%s\n %s\t%d\t%d\t%n\n", path, permission, length, modifyTime);
    }

    void append(String[] args) throws IOException {
        String opt = args[1];
        Path hdfsFilePath = new Path(args[2]);
        String content = args[3];
        // 向 HDFS 中指定的文件追加内容，由用户指定内容追加到原有文件的开头或结尾；

        if (opt.startsWith("e")) {
            FSDataOutputStream stream = fs.append(hdfsFilePath);
            stream.write(content.getBytes(StandardCharsets.UTF_8));
            stream.flush();
            stream.close();
        }
        if (opt.startsWith("b")){
            FSDataInputStream inputStream = fs.open(hdfsFilePath);
            byte[] buf = new byte[4096];
            int len;
            File tmp = File.createTempFile("tmp", null);
            FileOutputStream tmpOut = new FileOutputStream(tmp);
            tmpOut.write(content.getBytes(StandardCharsets.UTF_8));
            while((len = inputStream.read(buf)) > 0){
                tmpOut.write(buf, 0, len);
            }
            // org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "file"
            System.out.println("tmp at " + tmp.getPath());
            fs.copyFromLocalFile(true, true, new Path(tmp.getPath()), hdfsFilePath);
        }
    }

    void mv(String[] args) throws IOException {
        Path srcPath = new Path(args[1]);
        Path dstPath = new Path(args[2]);
        //在 HDFS 中，将文件从源路径移动到目的路径。
        fs.rename(srcPath, dstPath);
    }
}
