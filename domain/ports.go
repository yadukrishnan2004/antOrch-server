package domain

type WorkflowRepository interface{
	Save(wf *Workflow) error
	FindByID(id string) (*Workflow, error)
	Exists(id string) (bool, error)
}

type TaskQueue interface{
	Enqueue(task Task) error
}

type Task struct{
	WorkflowID string
	ActivityID string
	Name 	   string
	Input 	   interface{}
	RetryPolicy    RetryPolicy // rules for retrying on failure
	CurrentAttempt int
}

type ActivityResult struct{
	WorkflowID string
	ActivityID string
	Output     interface{}
	Err 	   error
}

type ActivityRegistry interface {
	Lookup(name string) (ActivityFunc, error)
}

type ActivityFunc func(input interface{}) (interface{}, error)