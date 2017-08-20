<?php

class Net_Gearman_Task
{
    public $func = '';
    public $arg = array();
    public $type = self::JOB_NORMAL;
    public $handle = '';
	public $server = '';
    public $uniq = '';
    public $finished = false;
    public $result = '';

    protected $callback = array(
        self::TASK_COMPLETE => array(),
        self::TASK_FAIL     => array(),
        self::TASK_STATUS   => array()
    );

    const JOB_NORMAL = 1;
    const JOB_BACKGROUND = 2;
    const JOB_HIGH = 2;
    const TASK_COMPLETE = 1;
    const TASK_FAIL = 2;
    const TASK_STATUS = 3;

    public function __construct($func, $arg, $uniq = null,
                                $type = self::JOB_NORMAL) 
    {
        $this->func = $func;
        $this->arg  = $arg;

        if (is_null($uniq)) {
            $this->uniq = md5($func . serialize($arg) . $type);
        } else {
            $this->uniq = $uniq;
        }

        $this->type = $type; 
    }

    public function attachCallback($callback, $type = self::TASK_COMPLETE) 
    {
        if (!is_callable($callback)) {
            throw new Net_Gearman_Exception('Invalid callback specified'); 
        } 

        $this->callback[$type][] = $callback;
    }

    public function complete($result)
    {
        $this->finished = true;
        $this->result   = $result;
        
        if (!count($this->callback[self::TASK_COMPLETE])) {
            return;
        }

        foreach ($this->callback[self::TASK_COMPLETE] as $callback) {
            call_user_func($callback, $this->func, $this->handle, $result);
        }
    }

    public function fail()
    {
        $this->finished = true;
        if (!count($this->callback[self::TASK_FAIL])) {
            return;
        }

        foreach ($this->callback[self::TASK_FAIL] as $callback) {
            call_user_func($callback, $this);
        }
    }

    public function status($numerator, $denominator)
    {
        if (!count($this->callback[self::TASK_STATUS])) {
            return;
        }

        foreach ($this->callback[self::TASK_STATUS] as $callback) {
            call_user_func($callback, 
                           $this->func, 
                           $this->handle, 
                           $numerator, 
                           $denominator);
        }
    }
}

class Net_Gearman_Set implements IteratorAggregate, Countable
{
    public $tasksCount = 0;
    public $tasks = array();
    public $handles = array();
    protected $callback = null;

    public function __construct(array $tasks = array())
    {
        foreach ($tasks as $task) {
            $this->addTask($task);
        }
    }

    public function addTask(Net_Gearman_Task $task)
    {
        if (!isset($this->tasks[$task->uniq])) {
            $this->tasks[$task->uniq] = $task;
            $this->tasksCount++;
        }
    }

    public function getTask($handle)
    {
        if (!isset($this->handles[$handle])) {
            throw new Net_Gearman_Exception('Unknown handle');
        }

        if (!isset($this->tasks[$this->handles[$handle]])) {
            throw new Net_Gearman_Exception('No task by that handle');
        }

        return $this->tasks[$this->handles[$handle]];
    }

    public function finished()
    {
        if ($this->tasksCount == 0) {
            if (isset($this->callback)) {
                foreach ($this->tasks as $task) {
                    $results[] = $task->result;
                }              

                call_user_func($this->callback, $results);
            }

            return true;
        }

        return false;
    }

    public function attachCallback($callback) 
    {
        if (!is_callable($callback)) {
            throw new Net_Gearman_Exception('Invalid callback specified'); 
        } 

        $this->callback = $callback;
    }

    public function getIterator()
    {
        return new ArrayIterator($this->tasks);
    }

    public function count()
    {
        return $this->tasksCount;
    }
}


class Net_Gearman_Client
{

    protected $conn = array();
    protected $connByServer = array();
    protected $servers = array();
    protected $timeout = 1000;

    public function __construct($servers, $timeout = 1000)
    {
        if (!is_array($servers) && strlen($servers)) {
            $servers = array($servers);
        } elseif (is_array($servers) && !count($servers)) {
            throw new Net_Gearman_Exception('Invalid servers specified');
        }

        $this->servers = $servers;
        foreach ($this->servers as $key => $server) {
            $conn = Net_Gearman_Connection::connect($server, $timeout);
            if (!Net_Gearman_Connection::isConnected($conn)) {
                unset($this->servers[$key]);
                continue;
            }
            $this->connByServer[$server] = $conn;
            $this->conn[] = $conn;
        }

        $this->timeout = $timeout;
    }

    protected function getConnection()
    {
        return $this->conn[array_rand($this->conn)];
    }

    public function __call($func, array $args = array())
    {
        $send = "";
        if (isset($args[0]) && !empty($args[0])) {
            $send = $args[0];
        }

        $task       = new Net_Gearman_Task($func, $send);
        $task->type = Net_Gearman_Task::JOB_BACKGROUND;

        $set = new Net_Gearman_Set();
        $set->addTask($task);
        $this->runSet($set);
        return $task->handle;
    }

    protected function submitTask(Net_Gearman_Task $task)
    {
        switch ($task->type) {
        case Net_Gearman_Task::JOB_BACKGROUND:
            $type = 'submit_job_bg';
            break;
        case Net_Gearman_Task::JOB_HIGH:
            $type = 'submit_job_high';
            break;
        default:
            $type = 'submit_job';
            break;
        }

        // if we don't have a scalar
        // json encode the data
        if(!is_scalar($task->arg)){
            $arg = json_encode($task->arg);
        } else {
            $arg = $task->arg;
        }

        $params = array(
            'func' => $task->func,
            'uniq' => $task->uniq,
            'arg'  => $arg
        );

        $s = $this->getConnection();
        Net_Gearman_Connection::send($s, $type, $params);

        if (!is_array(Net_Gearman_Connection::$waiting[(int)$s])) {
            Net_Gearman_Connection::$waiting[$s] = array();
        }

        array_push(Net_Gearman_Connection::$waiting[(int)$s], $task);
		
        $server = $this->getServerFromConnection($s);

        if ($server)
        {
            $task->server = $server;
        }

    }

    public function runSet(Net_Gearman_Set $set) 
    {
        $totalTasks = $set->tasksCount;
        $taskKeys   = array_keys($set->tasks);
        $t          = 0;

        while (!$set->finished()) {
            if ($t < $totalTasks) {
                $k = $taskKeys[$t];
                $this->submitTask($set->tasks[$k]);
                if ($set->tasks[$k]->type == Net_Gearman_Task::JOB_BACKGROUND) {
                    $set->tasks[$k]->finished = true;
                    $set->tasksCount--;
                }

                $t++;
            }

            $write  = null;
            $except = null;
            $read   = $this->conn;
            socket_select($read, $write, $except, 10);
            foreach ($read as $socket) {
                $resp = Net_Gearman_Connection::read($socket);
                if (count($resp)) {
                    $this->handleResponse($resp, $socket, $set);
                }
            }
        }
    }

    protected function handleResponse($resp, $s, Net_Gearman_Set $tasks) 
    {
        if (isset($resp['data']['handle']) && 
            $resp['function'] != 'job_created') {
            $task = $tasks->getTask($resp['data']['handle']);
        }

        switch ($resp['function']) {
        case 'work_complete':
            $tasks->tasksCount--;
            $task->complete(json_decode($resp['data']['result'], true));
            break;
        case 'work_status':
            $n = (int)$resp['data']['numerator'];
            $d = (int)$resp['data']['denominator'];
            $task->status($n, $d);
            break;
        case 'work_fail':
            $tasks->tasksCount--;
            $task->fail();
            break;
        case 'job_created':
            $task         = array_shift(Net_Gearman_Connection::$waiting[(int)$s]);
            $task->handle = $resp['data']['handle'];
            if ($task->type == Net_Gearman_Task::JOB_BACKGROUND) {
                $task->finished = true;
            }
            $tasks->handles[$task->handle] = $task->uniq;
            break;
        case 'error':
            throw new Net_Gearman_Exception('An error occurred');
        default:
            throw new Net_Gearman_Exception(
                'Invalid function ' . $resp['function']
            ); 
        }
    }

    protected function getServerFromConnection($connection)
    {
        foreach($this->connByServer as $server => $conn) {
            if ($conn === $connection) {
                return $server;
            }
        }

        return false;
    }

    protected function getConnectionFromServer($server)
    {
        if (isset($this->connByServer[$server])) {
            return $this->connByServer[$server];
        }

        return null;
    }

	public function disconnect()
    {
        if (!is_array($this->conn) || !count($this->conn)) {
            return;
        }

        foreach ($this->conn as $conn) {
            Net_Gearman_Connection::close($conn);
        }
    }

    public function __destruct()
    {
        $this->disconnect();
    }
}

class Net_Gearman_Connection
{

    static protected $commands = array(
        'can_do' => array(1, array('func')),
        'can_do_timeout' => array(23, array('func', 'timeout')),
        'cant_do' => array(2, array('func')),
        'reset_abilities' => array(3, array()),
        'set_client_id' => array(22, array('client_id')),
        'pre_sleep' => array(4, array()),
        'noop' => array(6, array()),
        'submit_job' => array(7, array('func', 'uniq', 'arg')),
        'submit_job_high' => array(21, array('func', 'uniq', 'arg')),
        'submit_job_bg' => array(18, array('func', 'uniq', 'arg')),
        'job_created' => array(8, array('handle')),
        'grab_job' => array(9, array()),
        'no_job' => array(10, array()),
        'job_assign' => array(11, array('handle', 'func', 'arg')),
        'work_status' => array(12, array('handle', 'numerator', 'denominator')),
        'work_complete' => array(13, array('handle', 'result')),
        'work_fail' => array(14, array('handle')),
        'get_status' => array(15, array('handle')),
        'status_res' => array(20, array('handle', 'known', 'running', 'numerator', 'denominator')),
        'echo_req' => array(16, array('text')),
        'echo_res' => array(17, array('text')),
        'error' => array(19, array('err_code', 'err_text')),
        'all_yours' => array(24, array())
    );
	
    static protected $magic = array();
    static public $waiting = array();
    static protected $multiByteSupport = null;

    final private function __construct()
    {
        // Don't allow this class to be instantiated
    }

    static public function connect($host, $timeout = 2000)
    {
        if (!count(self::$magic)) {
            foreach (self::$commands as $cmd => $i) {
                self::$magic[$i[0]] = array($cmd, $i[1]);
            }
        }

        $err   = '';
        $errno = 0;
        $port  = 7003;

        if (strpos($host, ':')) {
            list($host, $port) = explode(':', $host);
        }

        $start = microtime(true);
        do {
            $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
            @socket_connect($socket, $host, $port);
            $errorCode = socket_last_error($socket);
            
            socket_set_nonblock($socket);
            socket_set_option($socket, SOL_TCP, 1, 1);
            $timeLeft = ((microtime(true) - $start) * 1000);
        } while (!is_resource($socket) && $timeLeft < $timeout);

        if ($errorCode == 111) {
            throw new Net_Gearman_Exception("Can't connect to server");
        }

        self::$waiting[(int)$socket] = array();
        return $socket;
    }

    static public function send($socket, $command, array $params = array())
    {
        if (!isset(self::$commands[$command])) {
            throw new Net_Gearman_Exception('Invalid command: ' . $command);
        }

        $data = array();
        foreach (self::$commands[$command][1] as $field) {
            if (isset($params[$field])) {
                $data[] = $params[$field];
            }
        }

        $d = implode("\x00", $data);

        $cmd = "\0REQ" . pack("NN", 
                              self::$commands[$command][0], 
                              self::stringLength($d)) . $d;

        $cmdLength = self::stringLength($cmd); 
        $written = 0;
        $error = false;
        do {
            $check = @socket_write($socket, 
                                   self::subString($cmd, $written, $cmdLength), 
                                   $cmdLength);

            if ($check === false) {
                if (socket_last_error($socket) == SOCKET_EAGAIN or            
                    socket_last_error($socket) == SOCKET_EWOULDBLOCK or
                    socket_last_error($socket) == SOCKET_EINPROGRESS) 
                {
                  // skip this is okay
                }
                else
                {
                   $error = true;
                   break;   
                }
            }

            $written += (int)$check;
        } while ($written < $cmdLength);

        if ($error === true) {
            throw new Net_Gearman_Exception(
                'Could not write command to socket'
            );
        }
    }

    static public function read($socket)
    {
        $header = '';
        do {
            $buf = socket_read($socket, 12 - self::stringLength($header));
            $header .= $buf;
        } while ($buf !== false && 
                 $buf !== '' && self::stringLength($header) < 12);
        
        if ($buf === '') {
            throw new Net_Gearman_Exception("Connection was reset");
        }        

        if (self::stringLength($header) == 0) {
            return array();
        }
        $resp = @unpack('a4magic/Ntype/Nlen', $header);
       
        if (!count($resp) == 3) {
            throw new Net_Gearman_Exception('Received an invalid response');
        }

        if (!isset(self::$magic[$resp['type']])) {
            throw new Net_Gearman_Exception(
                'Invalid response magic returned: ' . $resp['type']
            );
        }

        $return = array();
        if ($resp['len'] > 0) {
            $data = '';
            while (self::stringLength($data) < $resp['len']) {
                $data .= socket_read($socket, $resp['len'] - self::stringLength($data));
            }

            $d = explode("\x00", $data);
            foreach (self::$magic[$resp['type']][1] as $i => $a) {
                $return[$a] = $d[$i]; 
            }
        }

		//var_dump( $return );
		
        $function = self::$magic[$resp['type']][0];
        if ($function == 'error') {

            if (!self::stringLength($return['err_text'])) {
                $return['err_text'] = 'Unknown error; see error code.';
            }

            throw new Net_Gearman_Exception(
                $return['err_text'], $return['err_code']
            );
        }

        return array('function' => self::$magic[$resp['type']][0],
                     'type' => $resp['type'],
                     'data' => $return);
    }

    static public function blockingRead($socket, $timeout = 500) 
    {
        static $cmds = array();

        $tv_sec  = floor(($timeout % 1000));
        $tv_usec = ($timeout * 1000);

        $start = microtime(true);
        while (count($cmds) == 0) { 
            if (((microtime(true) - $start) * 1000) > $timeout) {
                throw new Net_Gearman_Exception('Blocking read timed out');
            }

            $write  = null;
            $except = null;
            $read   = array($socket);

            socket_select($read, $write, $except, $tv_sec, $tv_usec);
            foreach ($read as $s) {
                $cmds[] = Net_Gearman_Connection::read($s);
            }
        } 

        return array_shift($cmds);
    }

    static public function close($socket)
    {
        if (is_resource($socket)) {
            socket_close($socket);
        }
    }

    static public function isConnected($conn)
    {
        return (is_null($conn) !== true &&
                is_resource($conn) === true && 
                strtolower(get_resource_type($conn)) == 'socket');
    }

    static public function stringLength($value)
    {
        if (is_null(self::$multiByteSupport)) {
            self::$multiByteSupport = intval(ini_get('mbstring.func_overload'));
        }

        if (self::$multiByteSupport & 2) { 
            return mb_strlen($value, '8bit');
        } else {
            return strlen($value);
        }
    }

    static public function subString($str, $start, $length)
    {
        if (is_null(self::$multiByteSupport)) {
            self::$multiByteSupport = intval(ini_get('mbstring.func_overload'));
        }

        if (self::$multiByteSupport & 2) { 
            return mb_substr($str, $start, $length, '8bit');
        } else {
            return substr($str, $start, $length);
        }
    }
}


class PEAR_Exception extends Exception
{
    const OBSERVER_PRINT = -2;
    const OBSERVER_TRIGGER = -4;
    const OBSERVER_DIE = -8;
    protected $cause;
    private static $_observers = array();
    private static $_uniqueid = 0;
    private $_trace;

    public function __construct($message, $p2 = null, $p3 = null)
    {
        if (is_int($p2)) {
            $code = $p2;
            $this->cause = null;
        } elseif (is_object($p2) || is_array($p2)) {
            // using is_object allows both Exception and PEAR_Error
            if (is_object($p2) && !($p2 instanceof Exception)) {
                if (!class_exists('PEAR_Error') || !($p2 instanceof PEAR_Error)) {
                    throw new PEAR_Exception(
                        'exception cause must be Exception, ' .
                        'array, or PEAR_Error'
                    );
                }
            }
            $code = $p3;
            if (is_array($p2) && isset($p2['message'])) {
                // fix potential problem of passing in a single warning
                $p2 = array($p2);
            }
            $this->cause = $p2;
        } else {
            $code = null;
            $this->cause = null;
        }
        parent::__construct($message, $code);
        $this->signal();
    }

    public static function addObserver($callback, $label = 'default')
    {
        self::$_observers[$label] = $callback;
    }

    public static function removeObserver($label = 'default')
    {
        unset(self::$_observers[$label]);
    }

    public static function getUniqueId()
    {
        return self::$_uniqueid++;
    }

    protected function signal()
    {
        foreach (self::$_observers as $func) {
            if (is_callable($func)) {
                call_user_func($func, $this);
                continue;
            }
            settype($func, 'array');
            switch ($func[0]) {
            case self::OBSERVER_PRINT :
                $f = (isset($func[1])) ? $func[1] : '%s';
                printf($f, $this->getMessage());
                break;
            case self::OBSERVER_TRIGGER :
                $f = (isset($func[1])) ? $func[1] : E_USER_NOTICE;
                trigger_error($this->getMessage(), $f);
                break;
            case self::OBSERVER_DIE :
                $f = (isset($func[1])) ? $func[1] : '%s';
                die(printf($f, $this->getMessage()));
                break;
            default:
                trigger_error('invalid observer type', E_USER_WARNING);
            }
        }
    }

    public function getErrorData()
    {
        return array();
    }

    public function getCause()
    {
        return $this->cause;
    }

    public function getCauseMessage(&$causes)
    {
        $trace = $this->getTraceSafe();
        $cause = array('class'   => get_class($this),
                       'message' => $this->message,
                       'file' => 'unknown',
                       'line' => 'unknown');
        if (isset($trace[0])) {
            if (isset($trace[0]['file'])) {
                $cause['file'] = $trace[0]['file'];
                $cause['line'] = $trace[0]['line'];
            }
        }
        $causes[] = $cause;
        if ($this->cause instanceof PEAR_Exception) {
            $this->cause->getCauseMessage($causes);
        } elseif ($this->cause instanceof Exception) {
            $causes[] = array('class'   => get_class($this->cause),
                              'message' => $this->cause->getMessage(),
                              'file' => $this->cause->getFile(),
                              'line' => $this->cause->getLine());
        } elseif (class_exists('PEAR_Error') && $this->cause instanceof PEAR_Error) {
            $causes[] = array('class' => get_class($this->cause),
                              'message' => $this->cause->getMessage(),
                              'file' => 'unknown',
                              'line' => 'unknown');
        } elseif (is_array($this->cause)) {
            foreach ($this->cause as $cause) {
                if ($cause instanceof PEAR_Exception) {
                    $cause->getCauseMessage($causes);
                } elseif ($cause instanceof Exception) {
                    $causes[] = array('class'   => get_class($cause),
                                   'message' => $cause->getMessage(),
                                   'file' => $cause->getFile(),
                                   'line' => $cause->getLine());
                } elseif (class_exists('PEAR_Error')
                    && $cause instanceof PEAR_Error
                ) {
                    $causes[] = array('class' => get_class($cause),
                                      'message' => $cause->getMessage(),
                                      'file' => 'unknown',
                                      'line' => 'unknown');
                } elseif (is_array($cause) && isset($cause['message'])) {
                    // PEAR_ErrorStack warning
                    $causes[] = array(
                        'class' => $cause['package'],
                        'message' => $cause['message'],
                        'file' => isset($cause['context']['file']) ?
                                            $cause['context']['file'] :
                                            'unknown',
                        'line' => isset($cause['context']['line']) ?
                                            $cause['context']['line'] :
                                            'unknown',
                    );
                }
            }
        }
    }

    public function getTraceSafe()
    {
        if (!isset($this->_trace)) {
            $this->_trace = $this->getTrace();
            if (empty($this->_trace)) {
                $backtrace = debug_backtrace();
                $this->_trace = array($backtrace[count($backtrace)-1]);
            }
        }
        return $this->_trace;
    }

    public function getErrorClass()
    {
        $trace = $this->getTraceSafe();
        return $trace[0]['class'];
    }

    public function getErrorMethod()
    {
        $trace = $this->getTraceSafe();
        return $trace[0]['function'];
    }

    public function __toString()
    {
        if (isset($_SERVER['REQUEST_URI'])) {
            return $this->toHtml();
        }
        return $this->toText();
    }

    public function toHtml()
    {
        $trace = $this->getTraceSafe();
        $causes = array();
        $this->getCauseMessage($causes);
        $html =  '<table style="border: 1px" cellspacing="0">' . "\n";
        foreach ($causes as $i => $cause) {
            $html .= '<tr><td colspan="3" style="background: #ff9999">'
               . str_repeat('-', $i) . ' <b>' . $cause['class'] . '</b>: '
               . htmlspecialchars($cause['message'])
                . ' in <b>' . $cause['file'] . '</b> '
               . 'on line <b>' . $cause['line'] . '</b>'
               . "</td></tr>\n";
        }
        $html .= '<tr><td colspan="3" style="background-color: #aaaaaa; text-align: center; font-weight: bold;">Exception trace</td></tr>' . "\n"
               . '<tr><td style="text-align: center; background: #cccccc; width:20px; font-weight: bold;">#</td>'
               . '<td style="text-align: center; background: #cccccc; font-weight: bold;">Function</td>'
               . '<td style="text-align: center; background: #cccccc; font-weight: bold;">Location</td></tr>' . "\n";
        foreach ($trace as $k => $v) {
            $html .= '<tr><td style="text-align: center;">' . $k . '</td>'
                   . '<td>';
            if (!empty($v['class'])) {
                $html .= $v['class'] . $v['type'];
            }
            $html .= $v['function'];
            $args = array();
            if (!empty($v['args'])) {
                foreach ($v['args'] as $arg) {
                    if (is_null($arg)) {
                        $args[] = 'null';
                    } else if (is_array($arg)) {
                        $args[] = 'Array';
                    } else if (is_object($arg)) {
                        $args[] = 'Object('.get_class($arg).')';
                    } else if (is_bool($arg)) {
                        $args[] = $arg ? 'true' : 'false';
                    } else if (is_int($arg) || is_double($arg)) {
                        $args[] = $arg;
                    } else {
                        $arg = (string)$arg;
                        $str = htmlspecialchars(substr($arg, 0, 16));
                        if (strlen($arg) > 16) {
                            $str .= '&hellip;';
                        }
                        $args[] = "'" . $str . "'";
                    }
                }
            }
            $html .= '(' . implode(', ', $args) . ')'
                   . '</td>'
                   . '<td>' . (isset($v['file']) ? $v['file'] : 'unknown')
                   . ':' . (isset($v['line']) ? $v['line'] : 'unknown')
                   . '</td></tr>' . "\n";
        }
        $html .= '<tr><td style="text-align: center;">' . ($k+1) . '</td>'
               . '<td>{main}</td>'
               . '<td>&nbsp;</td></tr>' . "\n"
               . '</table>';
        return $html;
    }

    public function toText()
    {
        $causes = array();
        $this->getCauseMessage($causes);
        $causeMsg = '';
        foreach ($causes as $i => $cause) {
            $causeMsg .= str_repeat(' ', $i) . $cause['class'] . ': '
                   . $cause['message'] . ' in ' . $cause['file']
                   . ' on line ' . $cause['line'] . "\n";
        }
        return $causeMsg . $this->getTraceAsString();
    }
}


class Net_Gearman_Exception extends PEAR_Exception
{

}
