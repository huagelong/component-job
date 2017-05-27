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

use Trensy\Component\Job\Cron\CronExpression;
use Trensy\Foundation\Storage\Redis;
use Trensy\Component\Job\Exception\InvalidArgumentException;
use Trensy\Server\Reload;
use Trensy\Support\Exception;
use Trensy\Support\Log;
use Trensy\Support\Exception\RuntimeExitException;

class Job
{


    const JOB_KEY_PRE = "JOB_KEY";

    private $config = [];

    /**
     * @var \Trensy\Foundation\Storage\Redis
     */
    private $storage = null;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->storage = new Redis();
    }

    /**
     * job 服务开始
     * @param $queueName
     */
    public function start($queueName)
    {
        if (!$this->config) return;

        $timeTick = isset($this->config['server']['timer_tick']) ? $this->config['server']['timer_tick'] : 500;
        $this->config['auto_reload'] = isset($this->config['server']['auto_reload'])?$this->config['server']['auto_reload']:false;

        while(true)
        {
            $this->run($queueName);
//            sleep(60);
            usleep($timeTick);
            Reload::perform($this->config['server_name'] . "-master", $this->config['server']['mem_reboot_rate'], $this->config);
        }
    }

    /**
     * job 服务执行
     * @param $queueName
     */
    private function run($queueName)
    {
        $checkKey = self::JOB_KEY_PRE .$queueName. "CHECK";
        $now = time();

        try {
            if (!isset($this->config['perform'][$queueName])) return;

            //原子操作避免重复处理,等待
            $this->checkLock($checkKey, $queueName);

            $key = self::JOB_KEY_PRE . ":" . $queueName;
            $data = $this->storage->zrangebyscore($key, 0, $now);
            $initKey ="INIT_".$key;
            $initData = $this->storage->zrangebyscore($initKey, 0, $now);
            if($data && is_array($data)){
                $data = array_merge($data, (array) $initData);
            }else{
                $data = $initData;
            }

//            debug($data);debug($queueName);

            if ($data && is_array($data)) {
                foreach ($data as $v) {
                    $value = $v;
                    if (!$value||!is_string($value)) {
                        Log::error("Job value null :" . $queueName);
                        continue;
                    }

                    $valueArr = unserialize($value);
                    $queueName = isset($valueArr[0]) ? $valueArr[0] : "";
                    $jobObj = isset($valueArr[1]) ? $valueArr[1] : "";
                    if (!is_object($jobObj)) {
                        Log::error("jobObj null :" . $queueName);
                        continue;
                    }
                    $jobObj->perform();
                }

                $this->storage->zremrangebyscore($key, 0, $now);
                $this->storage->zremrangebyscore($initKey, 0, $now);

                foreach ($data as $v) {
                    $value = $v;
                    if (!$value||!is_string($value)) {
                        Log::error("Job value null :" . $queueName);
                        continue;
                    }

                    $valueArr = unserialize($value);
                    $queueName = isset($valueArr[0]) ? $valueArr[0] : "";
                    $jobObj = isset($valueArr[1]) ? $valueArr[1] : "";
                    $schedule = isset($valueArr[3]) ? $valueArr[3] : "";
                    $isInit = isset($valueArr[4]) ? $valueArr[4] : "";
                    if (!is_object($jobObj)) {
                        Log::error("jobObj null :" . $queueName);
                        continue;
                    }
                    $jobObj->perform();

                    if ($schedule) {
                        $cron = CronExpression::factory($schedule);
                        $runTime = $cron->getNextRunDate()->format('Y-m-d H:i:s');
                        $this->add($queueName, $jobObj, $runTime, $schedule, $isInit);
                    }
                }

            }

//            debug($queueName."--delete");
            $this->storage->del($checkKey);
        } catch (RuntimeExitException $e){
            $this->storage->del($checkKey);
            Log::sysinfo("RuntimeExitException:".$e->getMessage());
        }catch (\Exception $e) {
            $this->storage->del($checkKey);
            Log::error("Job ERROR : \n" . Exception::formatException($e));
        } catch (\Error $e) {
            $this->storage->del($checkKey);
            Log::error("Job ERROR : \n" . Exception::formatException($e));
        }
    }

    protected function checkLock($checkKey, $queueName)
    {
        $pv = $this->config['perform'][$queueName];
        //原子操作避免重复处理
        $check = $this->storage->setnx($checkKey, 1);
//        debug($checkKey.":".$check."--lock");
        $ttl = isset($pv['expireat']) ? $pv['expireat'] : 60*5;
        $this->storage->expire($checkKey, $ttl);

        if (!$check) {
            $sleep = $pv['sleep'] ? $pv['sleep'] : 1;
            sleep($sleep);
            $this->checkLock($checkKey, $queueName);
        }
    }


    /**
     * 添加job
     * @param $queueName
     * @param $jobObj
     * @param string $runTime
     * @param string $schedule
     * @param string $tag
     * @throws InvalidArgumentException
     */
    public function add($queueName, $jobObj, $runTime = "", $schedule = "", $isInit=0)
    {
        if (!isset($this->config['perform'][$queueName])) return;

        $key = self::JOB_KEY_PRE . ":" . $queueName;

        if($isInit) $key = "INIT_".$key;

        $config = $this->config['perform'][$queueName];

        if ($config['only_one']) {
            $data = $this->storage->zrange($key, 0, 0);
            //            dump("--------------------job.total-------------------------");
            //            dump($data);
            if ($data) return;
        }

        if (!$runTime && !$schedule) {
            $runTime = time();
        } else {
            if (!$runTime) {
                $cron = CronExpression::factory($schedule);
                $runTime = $cron->getNextRunDate()->format('Y-m-d H:i:s');
            }
        }

        $runTime = is_string($runTime) ? strtotime($runTime) : $runTime;

        $value = [];
        $value[0] = $queueName;
        $value[1] = $jobObj;
        $value[2] = $runTime;
        $value[3] = $schedule;
        $value[4] = $isInit;
        $saveVale = serialize($value);

        try{
//            debug($saveVale);
            $result = $this->storage->zadd($key, $runTime, $saveVale);
            if(!$result) throw new \Exception("add job fail!");
        }catch (\Exception $e){
//            debug($saveVale);
            $this->storage->zadd($key, $runTime, $saveVale);
            Log::sysinfo($e->getMessage());
        }
    }

}