<?php
/**
 *  job处理
 *
 * Trensy Framework
 *
 * PHP Version 7
 *
 * @author          kaihui.wang <hpuwang@gmail.com>
 * @copyright      trensy, Inc.
 * @package         trensy/framework
 * @version         1.0.7
 */

namespace Trensy\Component\Job;

use Trensy\Foundation\Shortcut;
use Trensy\Support\Exception;
use Trensy\Support\Log;
use Trensy\Support\Exception\RuntimeExitException;

class Job
{

    use Shortcut;

    const JOB_KEY_PRE = "job-key";

    private $config = [];

    /**
     * @var \Trensy\Foundation\Storage\Redis
     */
    public $storage = null;

    public function __construct(array $config)
    {
        $this->config = $config;

    }

    /**
     * job 服务开始
     * @param $queueName
     */
    public function start($queueName)
    {
        if (!isset($this->config['jobs'][$queueName]) && $this->config['jobs'][$queueName]) return;

        swoole_timer_tick(1000, function()use($queueName){
            $this->run($queueName);
        });

    }

    /**
     * job 服务执行
     * @param $queueName
     */
    private function run($queueName)
    {

        try {
            $pv = $this->config['jobs'][$queueName];
            $ips =  isset($pv['ip'])&& $pv['ip']?$pv['ip']:[];
            if($ips){
                $realip = swoole_get_local_ip();
                $realip = current($realip);
                Log::sysinfo("local ip :". $realip);
                if(!in_array($realip, $ips)){
                    Log::sysinfo("local ip not allow run job");
                    return ;
                }
            }
            $this->runProcess($queueName);
        } catch (RuntimeExitException $e){
            Log::sysinfo("RuntimeExitException:".$e->getMessage());
        }catch (\Exception $e) {
            Log::error("Job ERROR : \n" . Exception::formatException($e));
        } catch (\Error $e) {
            Log::error("Job ERROR : \n" . Exception::formatException($e));
        }
    }


    protected function runProcess($queueName){
        $pv = $this->config['jobs'][$queueName];
        $rule = isset($pv['rule']) ? $pv['rule'] : null;
        $start = isset($pv['start']) ? $pv['start'] : null;
        $end = isset($pv['end']) ? $pv['end'] : null;

        if(!$rule) return $rule;


        if($start && (time() < strtotime($start))) return ;
        if($end && (time() > strtotime($end))) return ;

//        Log::debug(date('Y-m-d H:i:s')."|".date($rule));
        if(date('Y-m-d H:i:s') != date($rule)){
            return ;
        }

        $process = new \swoole_process(function(\swoole_process $worker) use ($queueName){
            $tmpName = $this->config['server_name']."-job-runprocess-".$queueName;
            $worker->name($tmpName);

            Log::sysinfo("$tmpName start ...");
            $job = new Job($this->config);

            $job->runOne($queueName);
            Log::sysinfo("$tmpName end ...");
            $worker->exit(0);
        }, false);
        $pid = $process->start();
        return $pid;
    }


    protected function runOne($queueName)
    {
        $class = isset($this->config['jobs'][$queueName]['class']) && $this->config['jobs'][$queueName]['class']?$this->config['jobs'][$queueName]['class']:null;
        if(!$class) return ;

        $jobObj = new $class();

        if (!is_object($jobObj)) {
            Log::error("jobObj unvalidate :" . $queueName);
        }

        $jobObj->perform();
        return true;
    }

}