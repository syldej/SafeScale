/*
 * Copyright 2018-2020, CS Systemes d'Information, http://www.c-s.fr
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package engine

import (
	"strings"
)

// results ...
type results map[string]StepResults

// Successful ...
func (r results) Successful() bool {
	if len(r) > 0 {
		for _, step := range r {
			if !step.Successful() {
				return false
			}
		}
	}
	return true
}

// AllErrorMessages ...
func (r results) AllErrorMessages() string {
	output := ""
	for _, step := range r {
		val := strings.TrimSpace(step.ErrorMessages())
		if val != "" {
			output += val + "\n"
		}
	}
	return output
}

// ErrorMessagesOfStep ...
func (r results) ErrorMessagesOfStep(name string) string {
	if step, ok := r[name]; ok {
		return step.ErrorMessages()
	}
	return ""
}

// ErrorMessagesOfHost ...
func (r results) ErrorMessagesOfHost(name string) string {
	output := ""
	for _, step := range r {
		for h, e := range step {
			if h == name {
				val := e.Error().Error()
				if val != "" {
					output += val + "\n"
				}
			}
		}
	}
	return output
}

// ResultsOfStep ...
func (r results) ResultsOfStep(name string) StepResults {
	if step, ok := r[name]; ok {
		return step
	}
	return StepResults{}
}

// Transpose reorganizes Results to be indexed by hosts (instead by steps normally)
func (r results) Transpose() results {
	t := results{}
	for step, results := range r {
		for h, sr := range results {
			t[h] = StepResults{step: sr}
		}
	}
	return t
}

// Keys returns the keys of the Results
func (r results) Keys() []string {
	keys := []string{}
	for k := range r {
		keys = append(keys, k)
	}
	return keys
}
