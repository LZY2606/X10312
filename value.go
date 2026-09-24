package formstream

import "slices"

// Value first value of the key.
func (p *Parser) Value(key string) (string, Header, bool) {
	p.valueMu.RLock()
	defer p.valueMu.RUnlock()
	value := p.valueMap[key]
	if len(value) == 0 {
		return "", Header{}, false
	}

	content, header := value[0].Unwrap()

	return content, header, true
}

// ValueRaw first value of the key.
func (p *Parser) ValueRaw(key string) ([]byte, Header, bool) {
	p.valueMu.RLock()
	defer p.valueMu.RUnlock()
	value := p.valueMap[key]
	if len(value) == 0 {
		return nil, Header{}, false
	}

	content, header := value[0].UnwrapRaw()

	return content, header, true
}

// Values all values of the key.
func (p *Parser) Values(key string) ([]Value, bool) {
	p.valueMu.RLock()
	defer p.valueMu.RUnlock()
	value, ok := p.valueMap[key]
	if !ok {
		return nil, false
	}

	return slices.Clone(value), true
}

// ValueMap all values.
func (p *Parser) ValueMap() map[string][]Value {
	p.valueMu.RLock()
	defer p.valueMu.RUnlock()
	out := make(map[string][]Value, len(p.valueMap))
	for k, v := range p.valueMap {
		out[k] = slices.Clone(v)
	}
	return out
}
