<?php
/**
 * Trensy Framework
 *
 * PHP Version 7
 *
 * @author          kaihui.wang <hpuwang@gmail.com>
 * @copyright      trensy, Inc.
 * @package         trensy/framework
 * @version         1.0.7
 */

namespace Trensy\Component\Job\Command;

use Trensy\Config\Config;
use Trensy\Component\Job\JobServer;
use Trensy\Support\Arr;
use Trensy\Support\Dir;
use Trensy\Support\ElapsedTime;
use Trensy\Support\Exception;
use Trensy\Support\Log;

class JobBase
{
    public static function operate($cmd, $output, $input)
    {
        ElapsedTime::setStartTime(ElapsedTime::SYS_START);
        $root = Dir::formatPath(ROOT_PATH);
        $configJob = Config::get("server.job");
        $appName = Config::get("server.name");

        if (!$appName) {
            Log::sysinfo("server.name not config");
            return ;
        }

        if (!$configJob) {
            Log::sysinfo("job config not config");
            return ;
        }

        $config = [];

        $config['jobs'] = $configJob;

        if ($input->hasOption("daemonize")) {
            $daemonize = $input->getOption('daemonize');
            $config['server']['daemonize'] = $daemonize == 0 ? 0 : 1;
        }

        try{
            self::doOperate($cmd, $config, $root, $appName, $output);
        }catch (\Exception $e){
            Log::error(Exception::formatException($e));
        }
    }


    public static function doOperate($command, array $config, $root, $appName, $output)
    {
        $defaultConfig = [
            //是否后台运行, 推荐设置0
            'daemonize' => 0,
            //worker数量，推荐设置和cpu核数相等
            'worker_num' => 2,
            "mem_reboot_rate" => 0.8,//可用内存达到多少自动重启
            "serialization" => 1
        ];

        $config['server'] = Arr::merge($defaultConfig, $config['server']);
        $config['server']['name'] = $appName;

        $serverName = $appName . "-job";
        $serverMaster = $appName . "-job-master";
        exec("ps axu|grep " . $serverMaster . "|grep -v grep|awk '{print $2}'", $masterPidArr);
        $masterPid = $masterPidArr ? current($masterPidArr) : null;

        if ($command === 'start' && $masterPid) {
            Log::sysinfo("$serverName already running");
            return;
        }

        if ($command !== 'start' && $command !== 'restart' && !$masterPid) {
            Log::sysinfo("$serverName not run");
            return;
        }
        // execute command.
        switch ($command) {
            case 'status':
                if ($masterPid) {
                    Log::sysinfo("$serverName already running");
                } else {
                    Log::sysinfo("$serverName not run");
                }
                break;
            case 'clear':
                $jobServer = new JobServer($config, $root);
                $jobServer->clear();
                Log::sysinfo("$serverName clear success ");
                break;
            case 'start':
                self::start($config, $root);
                break;
            case 'stop':
                self::stop($appName);
                Log::sysinfo("$serverName stop success ");
                break;
            case 'restart':
                $result = self::stop($appName);
                if($result){
                    self::start($config, $root);
                }
                break;
            case 'reload':
                self::reload($appName);
                Log::sysinfo("$serverName reload success ");
//                self::start($config, $root);
                break;
            default :
                exit(0);
        }
    }

    protected static function stop($appName)
    {
        $killStr = $appName . "-job";
        exec("ps axu|grep " . $killStr . "|grep -v grep|awk '{print $2}'|xargs kill -9", $out, $result);
        self::waitRunCmd("ps axu|grep " . $killStr . "|grep -v grep|awk '{print $2}'");
        return true;
    }

    protected static function waitRunCmd($cmd)
    {
        exec($cmd, $out, $result);
        if($out){
            sleep(1);
            self::waitRunCmd($cmd);
        }
        return true;
    }

    protected static function reload($appName)
    {
        $killStr = $appName . "-job-worker";
        $execStr = "ps axu|grep " . $killStr . "|grep -v grep|awk '{print $2}'|xargs kill -USR1";
        exec($execStr, $out, $result);
        return true;
    }

    protected static function start($config, $root)
    {
        $jobServer = new JobServer($config, $root);
        $jobServer->start();
    }

}