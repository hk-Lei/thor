import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

/**
 * @author moxingxing
 * @Date 2016/6/12
 */

public class PathMatcherDemo
{
    public static void main(String[] args)
    {
//        if (args.length != 2)
//        {
//            System.err.println("usage: java PatchMatcherDemo " +
//                    "syntax:pattern path");
//            return;
//        }

        String matcher = "regex:E:\\\\ideaProjects\\\\moxingxing\\\\thor\\\\data\\\\test_\\*\\.dat";
        String path = "E:\\ideaProjects\\moxingxing\\thor\\data\\test_1.dat";
        FileSystem fsDefault = FileSystems.getDefault();
        PathMatcher pm = fsDefault.getPathMatcher(matcher);

        System.out.println(fsDefault.getPath(path));

        if (pm.matches(fsDefault.getPath(path)))
            System.out.printf("%s matches pattern%n", path);
        else
            System.out.printf("%s doesn't match pattern%n", path);
    }
}