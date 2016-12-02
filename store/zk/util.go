package zk

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func any(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}
